// Thin DuckDB adapter over the public, standalone drive9 C++ HTTP client.
string ExtentPath(const string &input) {
	auto uri = NormalizeURI(input);
	if (uri.compare(0, 13, "tidbfs+db:///") == 0)
		return Path("tidbfs://" + uri.substr(12));
	return Path(uri);
}

struct ExtentClient {
	int64_t heartbeat_ms;
	std::shared_ptr<drive9::Client> client;
	std::unique_ptr<drive9::ExtentFileSystem> fs;
	explicit ExtentClient(const Config &config) : heartbeat_ms(config.heartbeat_ms) {
		try {
			drive9::ClientOptions client_options;
			client_options.endpoint = config.endpoint;
			client_options.token = config.token;
			client = std::make_shared<drive9::Client>(std::move(client_options));
			drive9::ExtentOptions options;
			options.heartbeat = std::chrono::milliseconds(config.heartbeat_ms);
			fs = std::make_unique<drive9::ExtentFileSystem>(client, options);
		} catch (const std::exception &error) {
			throw IOException("tidbfs extent: %s", error.what());
		}
	}
	static nlohmann::json Info(const drive9::FileInfo &info) {
		return {{"exists", info.exists}, {"directory", info.directory}, {"size", info.size}, {"inode", info.inode}};
	}
	nlohmann::json Call(const nlohmann::json &request) {
		try {
			auto op = request.at("op").get<std::string>();
			auto handle = request.value("handle", uint64_t(0));
			if (op == "stat") return Info(fs->Stat(request.at("path")));
			if (op == "open") {
				drive9::OpenOptions options;
				options.write = request.value("write", false);
				options.create = request.value("create", false);
				options.exclusive = request.value("exclusive", false);
				options.truncate = request.value("truncate", false);
				auto lock = request.value("lock", "");
				options.lock = lock == "read" ? drive9::Lock::Shared : lock == "write" ? drive9::Lock::Exclusive : drive9::Lock::None;
				auto id = fs->Open(request.at("path"), options);
				auto info = Info(fs->Info(id)); info["handle"] = id; return info;
			}
			if (op == "close") fs->Close(handle);
			else if (op == "sync") fs->Sync(handle);
			else if (op == "truncate") fs->Truncate(handle, request.at("offset"));
			else if (op == "mkdir") fs->Mkdir(request.at("path"));
			else if (op == "remove") fs->Remove(request.at("path"));
			else if (op == "rename") fs->Rename(request.at("path"), request.at("target"));
			else if (op == "list") {
				auto result = nlohmann::json::array();
				for (auto &entry : fs->List(request.at("path")))
					result.push_back({{"name", entry.name}, {"directory", entry.info.directory}});
				return {{"entries", result}};
			} else throw IOException("Unknown extent operation");
			return nlohmann::json::object();
		} catch (const std::exception &error) {
			throw IOException("tidbfs extent: %s", error.what());
		}
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
		try {
			return client->fs->ReadAt(handle, buffer, count, offset);
		} catch (const std::exception &error) {
			throw IOException("tidbfs extent: %s", error.what());
		}
	}
	void WriteAt(void *buffer, idx_t count, idx_t offset) {
		Check(true);
		if (offset > idx_t(std::numeric_limits<int64_t>::max()) ||
			count > idx_t(std::numeric_limits<int64_t>::max()) - offset)
			throw IOException("Extent write offset overflow");
		try {
			client->fs->WriteAt(handle, buffer, count, offset);
		} catch (const std::exception &error) {
			throw IOException("tidbfs extent: %s", error.what());
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
