// In-process adapter to the pinned drive9 extent runtime (bridge/main.go).
// Paths, blocks, sessions and locks are the existing filesystem's own objects.
extern "C" char *tidbfs_extent_call(char *request);
extern "C" void tidbfs_extent_free(char *response);
extern "C" char *tidbfs_extent_read(uint64_t session, uint64_t handle, void *buffer, uint64_t length,
                                    uint64_t offset, uint64_t *count);

nlohmann::json ExtentCall(nlohmann::json request) {
	auto raw = request.dump();
	std::unique_ptr<char, decltype(&tidbfs_extent_free)> response(tidbfs_extent_call(&raw[0]), tidbfs_extent_free);
	if (!response)
		throw IOException("Empty extent bridge response");
	auto result = nlohmann::json::parse(response.get());
	if (result.contains("error"))
		throw IOException("tidbfs extent: %s", JSONString(result, "error"));
	return result;
}

string ExtentPath(const string &input) {
	auto uri = NormalizeURI(input);
	if (uri.compare(0, 13, "tidbfs+db:///") == 0)
		return Path("tidbfs://" + uri.substr(12));
	return Path(uri);
}

struct ExtentClient {
	uint64_t session;
	int64_t heartbeat_ms;
	explicit ExtentClient(const Config &config) : heartbeat_ms(config.heartbeat_ms) {
		session = uint64_t(JSONNumber(
			ExtentCall({{"op", "connect"}, {"endpoint", config.endpoint}, {"token", config.token},
			            {"heartbeat_ms", config.heartbeat_ms}}), "session"));
	}
	~ExtentClient() {
		try {
			Call({{"op", "disconnect"}});
		} catch (...) {
		}
	}
	nlohmann::json Call(nlohmann::json request) {
		request["session"] = session;
		return ExtentCall(std::move(request));
	}
};

// Both URI routes belong to one DuckDB instance. Share session ownership and
// enforce its heartbeat setting even when another endpoint/credential is used.
struct ExtentClients {
	std::mutex mu;
	std::map<std::pair<string, string>, std::shared_ptr<ExtentClient>> clients;
	int64_t heartbeat_ms = 0;
	std::shared_ptr<ExtentClient> Get(const Config &config) {
		std::lock_guard<std::mutex> lock(mu);
		if (heartbeat_ms && heartbeat_ms != config.heartbeat_ms)
			throw InvalidInputException("Set tidbfs_heartbeat_ms before first extent access; use a fresh DuckDB instance to change it");
		auto &client = clients[{config.endpoint, config.token}];
		if (!client)
			client = std::make_shared<ExtentClient>(config);
		heartbeat_ms = config.heartbeat_ms;
		return client;
	}
};

struct ExtentHandle : FileHandle {
	std::shared_ptr<ExtentClient> client;
	uint64_t handle;
	idx_t position = 0, size;
	bool closed = false;
	std::shared_mutex mu;
	std::shared_ptr<ExtentHandle> read_pin;
	ExtentHandle(FileSystem &fs, const string &path, FileOpenFlags flags, std::shared_ptr<ExtentClient> c,
				 const nlohmann::json &info)
		: FileHandle(fs, path, flags), client(std::move(c)), handle(uint64_t(JSONNumber(info, "handle"))),
		  size(idx_t(JSONNumber(info, "size"))) {
		if (flags.OpenForAppending())
			position = size;
	}
	ExtentHandle(FileSystem &fs, const string &path, FileOpenFlags flags, std::shared_ptr<ExtentHandle> pin)
	    : FileHandle(fs, path, flags), client(pin->client), handle(pin->handle), size(pin->size), read_pin(std::move(pin)) { }
	~ExtentHandle() override {
		try {
			Close();
		} catch (...) {
		}
	}
	void Close() override {
		std::unique_lock<std::shared_mutex> lock(mu);
		if (closed)
			return;
		closed = true;
		if (read_pin)
			read_pin.reset();
		else
			client->Call({{"op", "close"}, {"handle", handle}});
	}
	void Check(bool writing = false) {
		if (closed)
			throw IOException("Extent handle is closed");
		if (writing && !flags.OpenForWriting())
			throw IOException("Extent handle is read only");
	}
	idx_t ReadAt(void *buffer, idx_t count, idx_t offset) {
		Check();
		if (offset > idx_t(std::numeric_limits<int64_t>::max()) ||
			count > idx_t(std::numeric_limits<int64_t>::max()) - offset)
			throw IOException("Extent read offset overflow");
		idx_t done = 0;
		while (done < count) {
			auto n = MinValue<idx_t>(1 << 20, count - done);
			uint64_t received = 0;
			std::unique_ptr<char, decltype(&tidbfs_extent_free)> error(
			    tidbfs_extent_read(client->session, handle, static_cast<char *>(buffer) + done, n, offset + done,
			                       &received), tidbfs_extent_free);
			if (error)
				throw IOException("tidbfs extent: %s", error.get());
			if (received > n)
				throw IOException("Oversized extent read");
			done += received;
			if (received < n)
				break;
		}
		return done;
	}
	void WriteAt(void *buffer, idx_t count, idx_t offset) {
		Check(true);
		if (offset > idx_t(std::numeric_limits<int64_t>::max()) ||
			count > idx_t(std::numeric_limits<int64_t>::max()) - offset)
			throw IOException("Extent write offset overflow");
		for (idx_t done = 0; done < count;) {
			auto n = MinValue<idx_t>(1 << 20, count - done);
			client->Call({{"op", "write"},
						  {"handle", handle},
						  {"offset", offset + done},
						  {"data", Blob::ToBase64(string_t(static_cast<const char *>(buffer) + done, n))}});
			done += n;
		}
		size = MaxValue<idx_t>(size, offset + count);
	}
};

class TiDBDatabaseFileSystem : public FileSystem {
	std::shared_ptr<ExtentClients> clients;
	std::shared_ptr<ExtentClient> Client(const string &path, optional_ptr<FileOpener> opener) {
		return clients->Get(ReadConfig(opener, path));
	}
	nlohmann::json Stat(const string &path, optional_ptr<FileOpener> opener) {
		return Client(path, opener)->Call({{"op", "stat"}, {"path", ExtentPath(path)}});
	}

	unique_ptr<ExtentHandle> OpenExtent(const string &path, FileOpenFlags flags, std::shared_ptr<ExtentClient> c) {
		auto p = ExtentPath(path);
		auto info = c->Call({{"op", "stat"}, {"path", p}});
		bool exists = info.at("exists").get<bool>();
		if ((!exists && flags.ReturnNullIfNotExists()) || (exists && flags.ReturnNullIfExists()))
			return nullptr;
		string lock;
		if (flags.Lock() == FileLockType::READ_LOCK)
			lock = "read";
		if (flags.Lock() == FileLockType::WRITE_LOCK)
			lock = "write";
		info = c->Call({{"op", "open"},
						{"path", p},
						{"write", flags.OpenForWriting()},
						{"create", flags.CreateFileIfNotExists() || flags.OverwriteExistingFile()},
						{"exclusive", flags.ExclusiveCreate()},
						{"truncate", flags.OverwriteExistingFile()},
						{"lock", lock}});
		return make_uniq<ExtentHandle>(*this, path, flags, std::move(c), info);
	}

  public:
	explicit TiDBDatabaseFileSystem(std::shared_ptr<ExtentClients> clients) : clients(std::move(clients)) { }
	string GetName() const override { return "tidbfs_extent"; }
	bool CanHandleFile(const string &path) override { return NormalizeURI(path).compare(0, 13, "tidbfs+db:///") == 0; }
	bool CanSeek() override { return true; }
	bool OnDiskFile(FileHandle &) override { return false; }
	bool IsManuallySet() override { return true; }
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		auto config = ReadConfig(opener, path);
		auto client = clients->Get(config);
		if (flags.OpenForReading() && !flags.OpenForWriting() && flags.Lock() == FileLockType::NO_LOCK) {
			// ATTACH probes the format with READ|NULL_IF_NOT_EXISTS before
			// opening the database with WRITE_LOCK. Its short-lived probe must
			// release its lock when closed, rather than retaining it to QueryEnd.
			if (flags.ReturnNullIfNotExists())
				return OpenExtent(path, flags | FileLockType::READ_LOCK, std::move(client));
			// Parquet/CSV readers request NO_LOCK. Pin one shared lock for the
			// whole query, including opens through the other URI and row groups
			// whose handles are opened after an earlier reader has closed.
			if (auto context = FileOpener::TryGetClientContext(opener)) {
				auto version = context->registered_state->GetOrCreate<QueryFiles>("tidbfs.query_files")
				                   ->Get(config, "tidbfs://" + ExtentPath(path));
				std::lock_guard<std::mutex> lock(version->mu);
				if (!version->extent) {
					auto pin = OpenExtent(path, flags | FileLockType::READ_LOCK, client);
					if (!pin)
						return nullptr;
					version->extent = std::shared_ptr<ExtentHandle>(pin.release());
				}
				return make_uniq<ExtentHandle>(*this, path, flags, version->extent);
			}
		}
		return OpenExtent(path, flags, std::move(client));
	}
	void Read(FileHandle &base, void *buffer, int64_t count, idx_t offset) override {
		auto &h = base.Cast<ExtentHandle>();
		std::shared_lock<std::shared_mutex> lock(h.mu);
		if (count < 0)
			throw IOException("Negative read size");
		auto received = h.ReadAt(buffer, idx_t(count), offset);
		if (received != idx_t(count))
			throw IOException("Short extent read from %s at %llu: expected %lld bytes, received %llu",
			                  h.path, offset, count, received);
	}
	int64_t Read(FileHandle &base, void *buffer, int64_t count) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		if (count < 0)
			throw IOException("Negative read size");
		auto n = h.ReadAt(buffer, idx_t(count), h.position);
		h.position += n;
		return int64_t(n);
	}
	void Write(FileHandle &base, void *buffer, int64_t count, idx_t offset) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		if (count < 0)
			throw IOException("Negative write size");
		h.WriteAt(buffer, idx_t(count), offset);
	}
	int64_t Write(FileHandle &base, void *buffer, int64_t count) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		if (count < 0)
			throw IOException("Negative write size");
		auto offset = h.flags.OpenForAppending() ? h.size : h.position;
		h.WriteAt(buffer, idx_t(count), offset);
		h.position = offset + idx_t(count);
		return count;
	}
	void FileSync(FileHandle &base) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		h.Check();
		h.client->Call({{"op", "sync"}, {"handle", h.handle}});
	}
	void Truncate(FileHandle &base, int64_t size) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		h.Check(true);
		if (size < 0)
			throw IOException("Negative truncate size");
		h.client->Call({{"op", "truncate"}, {"handle", h.handle}, {"offset", size}});
		h.size = idx_t(size);
	}
	int64_t GetFileSize(FileHandle &base) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		h.Check();
		return int64_t(h.size);
	}
	timestamp_t GetLastModifiedTime(FileHandle &) override { return timestamp_t(0); }
	FileType GetFileType(FileHandle &) override { return FileType::FILE_TYPE_REGULAR; }
	void Seek(FileHandle &base, idx_t offset) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		h.Check();
		h.position = offset;
	}
	idx_t SeekPosition(FileHandle &base) override {
		auto &h = base.Cast<ExtentHandle>();
		std::unique_lock<std::shared_mutex> lock(h.mu);
		h.Check();
		return h.position;
	}
	bool FileExists(const string &path, optional_ptr<FileOpener> opener) override {
		auto i = Stat(path, opener);
		return i.at("exists").get<bool>() && !i.at("directory").get<bool>();
	}
	bool DirectoryExists(const string &path, optional_ptr<FileOpener> opener) override {
		auto i = Stat(path, opener);
		return i.at("exists").get<bool>() && i.at("directory").get<bool>();
	}
	void CreateDirectory(const string &path, optional_ptr<FileOpener> opener) override {
		if (!DirectoryExists(path, opener))
			Client(path, opener)->Call({{"op", "mkdir"}, {"path", ExtentPath(path)}});
	}
	bool TryRemoveFile(const string &path, optional_ptr<FileOpener> opener) override {
		if (!FileExists(path, opener))
			return false;
		Client(path, opener)->Call({{"op", "remove"}, {"path", ExtentPath(path)}});
		return true;
	}
	void RemoveFile(const string &path, optional_ptr<FileOpener> opener) override {
		if (!TryRemoveFile(path, opener))
			throw IOException("Extent file does not exist");
	}
	bool ListFiles(const string &path, const std::function<void(const string &, bool)> &callback,
				   FileOpener *opener) override {
		if (!DirectoryExists(path, opener))
			return false;
		auto result = Client(path, opener)->Call({{"op", "list"}, {"path", ExtentPath(path)}});
		for (auto &entry : result.at("entries"))
			callback(JSONString(entry, "name"), entry.at("directory").get<bool>());
		return true;
	}
	void RemoveDirectory(const string &path, optional_ptr<FileOpener> opener) override {
		vector<std::pair<string, bool>> entries;
		ListFiles(path, [&](const string &name, bool dir) { entries.emplace_back(name, dir); }, opener.get());
		for (auto &entry : entries) {
			auto p = path + "/" + entry.first;
			if (entry.second)
				RemoveDirectory(p, opener);
			else
				RemoveFile(p, opener);
		}
		Client(path, opener)->Call({{"op", "remove"}, {"path", ExtentPath(path)}});
	}
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		auto a = ReadConfig(opener, source), b = ReadConfig(opener, target);
		if (a.endpoint != b.endpoint || a.token != b.token)
			throw IOException("Cannot move across filesystems");
		Client(source, opener)->Call({{"op", "rename"}, {"path", ExtentPath(source)}, {"target", ExtentPath(target)}});
	}
	vector<OpenFileInfo> Glob(const string &path, FileOpener *) override {
		if (HasGlob(path))
			throw NotImplementedException("Use tidbfs:/// for analytical globs");
		return {OpenFileInfo(path)};
	}
};
