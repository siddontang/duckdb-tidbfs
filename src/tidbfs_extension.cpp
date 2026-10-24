#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "json.hpp"

#include "duckdb/common/types/blob.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <thread>
#include <curl/curl.h>
#include <fnmatch.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <tuple>
#include <unistd.h>

namespace duckdb {
namespace {

constexpr idx_t MAX_READ = 8 * 1024 * 1024;
constexpr idx_t MAX_CACHE = 1024 * 1024;
constexpr idx_t MAX_LIST = 16 * 1024 * 1024;

// DuckDB's local JoinPath collapses the slashes when constructing a temporary
// sibling of a file in a URI's root (scheme:///file -> scheme:/tmp_file).
string NormalizeURI(const string &uri) {
	for (const string &scheme : {string("tidbfs"), string("tidbfs+db")}) {
		auto prefix = scheme + ":/";
		if (uri.compare(0, prefix.size(), prefix) == 0 &&
		    (uri.size() == prefix.size() || uri[prefix.size()] != '/')) {
			return scheme + "://" + uri.substr(scheme.size() + 1);
		}
	}
	return uri;
}

string Path(const string &uri) {
	if (NormalizeURI(uri) != uri) return Path(NormalizeURI(uri));
	if (uri.compare(0, 10, "tidbfs:///") != 0) {
		throw InvalidInputException("Use an absolute tidbfs:///path URI");
	}
	auto path = uri.substr(9);
	if (path.find('\\') != string::npos || path.find('\0') != string::npos ||
	    path.find("//") != string::npos) {
		throw InvalidInputException("Invalid tidbfs path");
	}
	for (auto &part : StringUtil::Split(path, '/')) {
		if (part == "." || part == "..") {
			throw InvalidInputException("Invalid tidbfs path component");
		}
	}
	return path;
}

string EncodePath(const string &path) {
	static const char hex[] = "0123456789ABCDEF";
	string result;
	for (unsigned char c : path) {
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		    c == '/' || c == '-' || c == '_' || c == '.' || c == '~') {
			result += char(c);
		} else {
			result += '%';
			result += hex[c >> 4];
			result += hex[c & 15];
		}
	}
	return result;
}

string SourceHeader(const string &path) {
	static const char alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
	string result;
	unsigned value = 0;
	int bits = 0;
	for (unsigned char byte : path) {
		value = (value << 8) | byte;
		bits += 8;
		while (bits >= 6) {
			bits -= 6;
			result += alphabet[(value >> bits) & 63];
		}
	}
	if (bits) {
		result += alphabet[(value << (6 - bits)) & 63];
	}
	return result;
}

string DirectoryPath(const string &uri) {
	auto path = Path(uri);
	if (path.back() != '/') {
		path += '/';
	}
	return path;
}

int64_t Number(const string &text) {
	int64_t value = 0;
	auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
	if (text.empty() || parsed.ec != std::errc() || parsed.ptr != text.data() + text.size() || value < 0) {
		throw IOException("Invalid numeric tidbfs response header");
	}
	return value;
}

struct URL {
	CURLU *value = curl_url();
	explicit URL(const string &text) {
		if (!value || curl_url_set(value, CURLUPART_URL, text.c_str(), 0) != CURLUE_OK) {
			if (value) {
				curl_url_cleanup(value);
			}
			throw InvalidInputException("Invalid tidbfs HTTP URL");
		}
	}
	~URL() { curl_url_cleanup(value); }
	string Get(CURLUPart part) const {
		char *out = nullptr;
		if (curl_url_get(value, part, &out, 0) != CURLUE_OK) {
			return "";
		}
		string result(out);
		curl_free(out);
		return result;
	}
	bool Loopback() const {
		auto host = Get(CURLUPART_HOST);
		return host == "localhost" || host == "127.0.0.1" || host == "[::1]";
	}
};

void CheckEndpoint(const string &endpoint) {
	URL url(endpoint);
	if ((!url.Get(CURLUPART_USER).empty()) || !url.Get(CURLUPART_PASSWORD).empty() ||
	    !url.Get(CURLUPART_QUERY).empty() || !url.Get(CURLUPART_FRAGMENT).empty() ||
	    (url.Get(CURLUPART_SCHEME) != "https" && !(url.Get(CURLUPART_SCHEME) == "http" && url.Loopback()))) {
		throw InvalidInputException("tidbfs endpoint requires HTTPS (HTTP is allowed only on loopback)");
	}
}

string ObjectURL(const string &endpoint, const string &location) {
	URL base(endpoint);
	URL target(endpoint);
	if (location.empty() || curl_url_set(target.value, CURLUPART_URL, location.c_str(), 0) != CURLUE_OK) {
		throw IOException("Invalid tidbfs object redirect");
	}
	bool local = base.Get(CURLUPART_SCHEME) == "http" && base.Loopback() &&
	             target.Get(CURLUPART_SCHEME) == "http" && target.Get(CURLUPART_HOST) == base.Get(CURLUPART_HOST) &&
	             target.Get(CURLUPART_PORT) == base.Get(CURLUPART_PORT);
	if ((target.Get(CURLUPART_SCHEME) != "https" && !local) || !target.Get(CURLUPART_USER).empty() ||
	    !target.Get(CURLUPART_PASSWORD).empty() || !target.Get(CURLUPART_FRAGMENT).empty()) {
		throw IOException("Unsafe tidbfs object redirect");
	}
	return target.Get(CURLUPART_URL);
}

struct Config {
	string endpoint;
	string token;
	idx_t read_ahead = 65536;
	int64_t heartbeat_ms = 12000;
};

// DuckDB can open one handle per row group. Keep version and object resolution
// query-local, so those handles use the same generation without N HEAD/redirects.
// Byte caches remain handle-local: their lifetime and memory scale with active
// readers, not with every file ever mentioned by a connection.
struct ExtentHandle;

struct FileVersion {
	std::mutex mu;
	bool initialized = false;
	idx_t size = 0;
	int64_t mtime = 0;
	string etag, object_url, object_etag;
	// Extent files use the existing shared lock instead of immutable objects.
	// Keep its owning handle until QueryEnd, across all row-group handles.
	std::shared_ptr<ExtentHandle> extent;
};

struct QueryFiles : ClientContextState {
	std::mutex mu;
	std::map<std::tuple<string, string, string>, shared_ptr<FileVersion>> files;
	shared_ptr<FileVersion> Get(const Config &config, const string &path) {
		std::lock_guard<std::mutex> lock(mu);
		auto &version = files[std::make_tuple(config.endpoint, config.token, path)];
		if (!version) {
			version = make_shared_ptr<FileVersion>();
		}
		return version;
	}
	void QueryEnd() override {
		std::lock_guard<std::mutex> lock(mu);
		files.clear();
	}
};

Config ReadConfig(optional_ptr<FileOpener> opener, const string &path) {
	auto manager = FileOpener::TryGetSecretManager(opener);
	auto transaction = FileOpener::TryGetCatalogTransaction(opener);
	if (!manager || !transaction) {
		throw IOException("tidbfs requires a DuckDB secret and a file opener");
	}
	auto match = manager->LookupSecret(*transaction, NormalizeURI(path), "tidbfs");
	if (!match.HasMatch()) {
		throw IOException("Create a tidbfs secret with ENDPOINT and TOKEN before opening files");
	}
	// The tidbfs secret factory and deserializer both produce KeyValueSecret.
	auto &secret = static_cast<const KeyValueSecret &>(match.GetSecret());
	Value endpoint, token, read_ahead, heartbeat;
	if (!secret.TryGetValue("endpoint", endpoint) || !secret.TryGetValue("token", token)) {
		throw IOException("tidbfs secret is missing ENDPOINT or TOKEN");
	}
	Config config;
	config.endpoint = endpoint.GetValue<string>();
	config.token = token.GetValue<string>();
	CheckEndpoint(config.endpoint);
	if (config.token.empty() || config.token.find_first_of("\r\n") != string::npos) {
		throw InvalidInputException("Invalid tidbfs token");
	}
	while (!config.endpoint.empty() && config.endpoint.back() == '/') {
		config.endpoint.pop_back();
	}
	if (FileOpener::TryGetCurrentSetting(opener, "tidbfs_read_ahead", read_ahead)) {
		auto size = read_ahead.GetValue<int64_t>();
		if (size < 0 || size > int64_t(MAX_CACHE)) {
			throw InvalidInputException("tidbfs_read_ahead must be between 0 and 1048576 bytes");
		}
		config.read_ahead = idx_t(size);
	}
	if (FileOpener::TryGetCurrentSetting(opener, "tidbfs_heartbeat_ms", heartbeat)) {
		config.heartbeat_ms = heartbeat.GetValue<int64_t>();
		if (config.heartbeat_ms < 1000 || config.heartbeat_ms > 600000) {
			throw InvalidInputException("tidbfs_heartbeat_ms must be between 1000 and 600000");
		}
	}
	return config;
}

struct Response {
	long status = 0;
	string body;
	std::map<string, string> headers;
	idx_t limit;
	string Header(const string &name) const {
		auto it = headers.find(name);
		return it == headers.end() ? "" : it->second;
	}
};

class HTTP {
public:
	HTTP() : curl(curl_easy_init()) {
		if (!curl) {
			throw IOException("Cannot initialize tidbfs HTTP client");
		}
	}
	~HTTP() { curl_easy_cleanup(curl); }
	HTTP(const HTTP &) = delete;
	HTTP &operator=(const HTTP &) = delete;

	Response Request(const string &url, const string &token, bool head, const vector<string> &headers, idx_t limit,
	                 const char *method = nullptr, const string &body = "", int fd = -1, idx_t offset = 0,
	                 idx_t length = 0) {
		Response response;
		UploadSource source {fd, offset, length};
		response.limit = MaxValue<idx_t>(limit, 65536); // bounded redirect/error bodies
		curl_easy_reset(curl); // preserves this handle's connection pool
		// Drain whole large-MSS loopback packets. Smaller receive batches can
		// collapse Linux's advertised window and leave range reads probing slowly.
		curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 65536L);
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_NOBODY, head ? 1L : 0L);
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
		curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
		curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, 120L);
		curl_easy_setopt(curl, CURLOPT_HTTP_CONTENT_DECODING, 0L);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, "duckdb-tidbfs/0.2.0");
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, Body);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, Header);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, &response);
		if (fd >= 0) {
			curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
			curl_easy_setopt(curl, CURLOPT_READFUNCTION, Upload);
			curl_easy_setopt(curl, CURLOPT_READDATA, &source);
			curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, curl_off_t(length));
		} else if (method) {
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.data());
			curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, curl_off_t(body.size()));
		}
		if (method) {
			curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);
		}
		curl_slist *list = nullptr;
		list = curl_slist_append(list, "Accept-Encoding: identity");
		list = curl_slist_append(list, "Expect:");
		if (!token.empty()) {
			list = curl_slist_append(list, ("Authorization: Bearer " + token).c_str());
		}
		for (auto &h : headers) {
			list = curl_slist_append(list, h.c_str());
		}
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
		auto code = curl_easy_perform(curl);
		curl_slist_free_all(list);
		if (code != CURLE_OK) {
			// Never include URLs, server bodies or curl's error buffer: signed URLs contain credentials.
			throw IOException("tidbfs HTTP transport failed (code %d)", int(code));
		}
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.status);
		return response;
	}

private:
	CURL *curl;
	struct UploadSource {
		int fd;
		idx_t offset, remaining;
	};
	static size_t Upload(char *data, size_t size, size_t count, void *user) noexcept {
		auto &source = *static_cast<UploadSource *>(user);
		auto length = MinValue<idx_t>(size * count, source.remaining);
		if (!length) {
			return 0;
		}
		ssize_t n;
		do {
			n = pread(source.fd, data, length, off_t(source.offset));
		} while (n < 0 && errno == EINTR);
		if (n <= 0) {
			return CURL_READFUNC_ABORT;
		}
		source.offset += idx_t(n);
		source.remaining -= idx_t(n);
		return size_t(n);
	}
	static size_t Body(char *data, size_t size, size_t count, void *user) noexcept {
		auto &r = *static_cast<Response *>(user);
		auto length = size * count;
		if (length > r.limit - r.body.size()) {
			return 0;
		}
		try {
			r.body.append(data, length);
			return length;
		} catch (...) {
			return 0;
		}
	}
	static size_t Header(char *data, size_t size, size_t count, void *user) noexcept {
		auto &r = *static_cast<Response *>(user);
		auto length = size * count;
		try {
			string line(data, length);
			if (line.compare(0, 5, "HTTP/") == 0) {
				r.headers.clear(); // e.g. CONNECT or interim 100 response
			}
			auto colon = line.find(':');
			if (colon != string::npos) {
				auto value = line.substr(colon + 1);
				StringUtil::Trim(value);
				r.headers[StringUtil::Lower(line.substr(0, colon))] = value;
			}
			return length;
		} catch (...) {
			return 0;
		}
	}
};

void CheckStatus(const Response &r) {
	if (r.status == 412) {
		throw IOException("tidbfs file changed since it was opened; restart the query");
	}
	if (r.status == 409) {
		throw IOException("tidbfs write conflict; destination changed or an upload is already active");
	}
	if (r.status < 200 || r.status >= 300) {
		throw IOException("tidbfs HTTP %d", int(r.status));
	}
}

nlohmann::json JSON(const Response &r) {
	CheckStatus(r);
	auto json = nlohmann::json::parse(r.body, nullptr, false);
	if (!json.is_object()) {
		throw IOException("Invalid tidbfs JSON response");
	}
	return json;
}

string JSONString(const nlohmann::json &json, const char *key) {
	if (!json.contains(key) || !json[key].is_string()) {
		throw IOException("Invalid tidbfs response field: %s", key);
	}
	return json[key].get<std::string>();
}

int64_t JSONNumber(const nlohmann::json &json, const char *key) {
	if (!json.contains(key) || !json[key].is_number_integer()) {
		throw IOException("Invalid tidbfs response field: %s", key);
	}
	return Number(json[key].dump());
}

Response PostJSON(HTTP &http, const Config &config, const string &route, const nlohmann::json &body) {
	return http.Request(config.endpoint + route, config.token, false, {"Content-Type: application/json"}, MAX_LIST,
	                    "POST", body.dump());
}

// Spill writable handles to an unlinked private local file. Memory stays bounded
// even for large COPY output; destruction only discards uncommitted bytes.
struct Spool {
	FILE *file = std::tmpfile();
	Spool() {
		if (!file) {
			throw IOException("Cannot create tidbfs staging file");
		}
	}
	~Spool() { std::fclose(file); }
	int FD() const { return fileno(file); }
	void IO(void *buffer, idx_t length, idx_t offset, bool write) {
		auto bytes = static_cast<char *>(buffer);
		while (length) {
			auto amount = MinValue(length, MAX_READ);
			auto n = write ? pwrite(FD(), bytes, amount, off_t(offset)) : pread(FD(), bytes, amount, off_t(offset));
			if (n < 0 && errno == EINTR) {
				continue;
			}
			if (n <= 0) {
				throw IOException("tidbfs local staging I/O failed");
			}
			bytes += n;
			offset += idx_t(n);
			length -= idx_t(n);
		}
	}
};

// Publish one complete generation. Never retry an ambiguous commit: a failed
// response may have followed a successful server-side publication.
int64_t Publish(HTTP &http, const Config &config, const string &path, Spool &spool, idx_t size, int64_t revision) {
	auto status = JSON(http.Request(config.endpoint + "/v1/status", config.token, false, {}, MAX_LIST));
	auto threshold = idx_t(JSONNumber(status, "inline_threshold"));
	if (status.contains("max_upload_bytes") && size > idx_t(JSONNumber(status, "max_upload_bytes"))) {
		throw IOException("tidbfs output exceeds the server's upload limit");
	}
	if (!size || size < threshold) {
		auto r = http.Request(config.endpoint + "/v1/fs" + EncodePath(Path(path)), config.token, false,
		                      {"Content-Type: application/octet-stream", "X-Dat9-Expected-Revision: " + std::to_string(revision)},
		                      MAX_LIST, "PUT", "", spool.FD(), 0, size);
		CheckStatus(r);
		if (r.status != 200 && r.status != 201) {
			throw IOException("tidbfs expected a completed inline write");
		}
		auto committed = JSONNumber(JSON(r), "revision");
		if (committed <= 0) {
			throw IOException("tidbfs write response lacks a committed revision");
		}
		return committed;
	}
	auto plan = JSON(PostJSON(http, config, "/v2/uploads/initiate",
	                          {{"path", Path(path)}, {"total_size", size}, {"expected_revision", revision}}));
	auto id = JSONString(plan, "upload_id");
	if (id.empty() || id.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != string::npos) {
		throw IOException("Invalid tidbfs upload ID");
	}
	auto route = "/v2/uploads/" + id;
	try {
		auto part_size = idx_t(JSONNumber(plan, "part_size"));
		if (!part_size || (size - 1) / part_size >= 10000) {
			throw IOException("Invalid tidbfs upload part size");
		}
		if (plan.contains("checksum_contract")) {
			auto &contract = plan["checksum_contract"];
			if (!contract.is_object() || !contract.contains("required") || !contract["required"].is_boolean()) {
				throw IOException("Invalid tidbfs upload checksum contract");
			}
			if (contract["required"].get<bool>()) {
				throw NotImplementedException("tidbfs server requires an unsupported upload checksum contract");
			}
		}
		nlohmann::json parts = nlohmann::json::array();
		for (idx_t offset = 0, number = 1; offset < size; number++) {
			auto length = MinValue(part_size, size - offset);
			Response uploaded;
			for (int attempt = 0; attempt < 2; attempt++) {
				auto part = JSON(PostJSON(http, config, route + "/presign", {{"part_number", number}}));
				if (JSONNumber(part, "number") != int64_t(number) || JSONNumber(part, "size") != int64_t(length)) {
					throw IOException("Invalid tidbfs presigned part bounds");
				}
				vector<string> headers;
				if (part.contains("headers") && !part["headers"].is_null()) {
					if (!part["headers"].is_object()) {
						throw IOException("Invalid tidbfs presigned headers");
					}
					for (auto item = part["headers"].begin(); item != part["headers"].end(); ++item) {
						auto key = StringUtil::Lower(item.key());
						if (key != "content-type" && key.compare(0, 6, "x-amz-") != 0 &&
						    key.compare(0, 5, "x-ms-") != 0 && key.compare(0, 7, "x-goog-") != 0) {
							continue;
						}
						if (!item.value().is_string() || key.find_first_of("\r\n:") != string::npos) {
							throw IOException("Invalid tidbfs presigned header");
						}
						auto value = item.value().get<std::string>();
						if (value.find_first_of("\r\n") != string::npos) {
							throw IOException("Invalid tidbfs presigned header value");
						}
						headers.push_back(item.key() + ": " + value);
					}
				}
				uploaded = http.Request(ObjectURL(config.endpoint, JSONString(part, "url")), "", false, headers,
				                        65536, "PUT", "", spool.FD(), offset, length);
				if (uploaded.status != 401 && uploaded.status != 403) {
					break;
				}
			}
			CheckStatus(uploaded);
			auto etag = uploaded.Header("etag");
			if (etag.empty()) {
				throw IOException("tidbfs uploaded part is missing its ETag");
			}
			parts.push_back({{"number", number}, {"etag", etag}});
			offset += length;
		}
		auto completed = PostJSON(http, config, route + "/complete", {{"parts", parts}});
		CheckStatus(completed);
		if (completed.status != 200) {
			throw IOException("tidbfs expected a completed multipart publication");
		}
	} catch (...) {
		try {
			PostJSON(http, config, route + "/abort", nlohmann::json::object());
		} catch (...) {
			// Best effort; the server expires abandoned sessions. Preserve the original error.
		}
		throw;
	}
	return revision + 1;
}

struct DeferredWrites : ClientContextState {
	struct Entry {
		Config config;
		string path;
		unique_ptr<Spool> spool;
		idx_t size;
		int64_t revision;
	};
	std::mutex mu;
	vector<Entry> entries;
	bool failed = false;
	std::set<std::tuple<string, string, string>> preserve_on_cleanup;
	bool preserve_all = false;
	void QueryBegin(ClientContext &) override {
		std::lock_guard<std::mutex> lock(mu);
		preserve_on_cleanup.clear();
		preserve_all = false;
	}
	void Preserve(const Config &config, const string &path) noexcept {
		std::lock_guard<std::mutex> lock(mu);
		try {
			preserve_on_cleanup.emplace(config.endpoint, config.token, NormalizeURI(path));
		} catch (...) {
			preserve_all = true;
		}
	}
	bool PreserveOnCleanup(const Config &config, const string &path) {
		std::lock_guard<std::mutex> lock(mu);
		return preserve_all || preserve_on_cleanup.count({config.endpoint, config.token, NormalizeURI(path)});
	}
	void Add(Config config, string path, unique_ptr<Spool> spool, idx_t size, int64_t revision) noexcept {
		std::lock_guard<std::mutex> lock(mu);
		try {
			entries.push_back({std::move(config), std::move(path), std::move(spool), size, revision});
		} catch (...) {
			failed = true;
		}
	}
	void QueryEnd(ClientContext &, optional_ptr<ErrorData> error) override {
		vector<Entry> pending;
		bool allocation_failed;
		{
			std::lock_guard<std::mutex> lock(mu);
			pending.swap(entries);
			allocation_failed = failed;
			failed = false;
		}
		if (error || (pending.empty() && !allocation_failed)) {
			return;
		}
		if (allocation_failed) {
			throw IOException("Could not retain tidbfs pending output");
		}
		HTTP http;
		for (auto &entry : pending) {
			try {
				Publish(http, entry.config, entry.path, *entry.spool, entry.size, entry.revision);
			} catch (...) {
				Preserve(entry.config, entry.path);
				throw;
			}
		}
	}
};

struct TiDBHandle : FileHandle {
	TiDBHandle(FileSystem &fs, const string &path, FileOpenFlags flags, Config config, shared_ptr<FileVersion> version)
	    : FileHandle(fs, path, flags), config(std::move(config)), version(std::move(version)) {
	}
	~TiDBHandle() override {
		// EXPORT's schema/load writers destroy handles without calling Close.
		// Defer their publication to QueryEnd, where errors can be reported.
		// Never perform fallible network I/O in a noexcept destructor.
		if (spool && dirty && !failed && deferred) {
			deferred->Add(std::move(config), std::move(path), std::move(spool), size, revision);
		}
	}
	Config config;
	shared_ptr<FileVersion> version;
	HTTP http;
	std::mutex mu;
	idx_t size = 0, position = 0, cache_start = 0;
	int64_t mtime = 0;
	string etag, cache;
	bool closed = false;
	bool dirty = false, failed = false;
	int64_t revision = 0;
	unique_ptr<Spool> spool;
	shared_ptr<DeferredWrites> deferred;
	string Endpoint() const { return config.endpoint + "/v1/fs" + EncodePath(Path(path)); }
	void Sync() {
		if (closed || failed) {
			throw IOException("tidbfs handle is closed or an earlier write failed");
		}
		if (!dirty) {
			return;
		}
		try {
			revision = Publish(http, config, path, *spool, size, revision);
			dirty = false;
		} catch (...) {
			failed = true; // Do not silently retry an ambiguous publication on Close.
			deferred->Preserve(config, path);
			throw;
		}
	}
	void Close() override {
		std::lock_guard<std::mutex> lock(mu);
		if (closed) {
			return;
		}
		Sync();
		closed = true;
		cache.clear();
		spool.reset();
	}
	void WriteAt(void *buffer, idx_t count, idx_t offset) {
		if (closed || failed || !spool) {
			throw IOException("tidbfs handle is not writable");
		}
		if (offset > idx_t(std::numeric_limits<int64_t>::max()) ||
		    count > idx_t(std::numeric_limits<int64_t>::max()) - offset) {
			throw IOException("tidbfs write exceeds supported file size");
		}
		if (!count) {
			return;
		}
		try {
			spool->IO(buffer, count, offset, true);
			size = MaxValue(size, offset + count);
			dirty = true;
		} catch (...) {
			failed = true;
			throw;
		}
	}
	Response Resolve(idx_t offset, idx_t count) {
		auto range = "Range: bytes=" + std::to_string(offset) + "-" + std::to_string(offset + count - 1);
		auto r = http.Request(Endpoint(), config.token, false, {range, "If-Match: " + etag}, count);
		if (r.Header("etag") != etag && r.status < 400) {
			throw IOException("tidbfs file version changed or server lacks conditional reads");
		}
		if (r.status == 302 || r.status == 307) {
			{
				std::lock_guard<std::mutex> lock(version->mu);
				version->object_url = ObjectURL(Endpoint(), r.Header("location"));
			}
			return ReadObject(offset, count);
		}
		return r;
	}
	Response ReadObject(idx_t offset, idx_t count) {
		vector<string> headers {"Range: bytes=" + std::to_string(offset) + "-" + std::to_string(offset + count - 1)};
		string object_url;
		{
			std::lock_guard<std::mutex> lock(version->mu);
			object_url = version->object_url;
			if (!version->object_etag.empty()) {
				headers.push_back("If-Match: " + version->object_etag);
			}
		}
		if (object_url.empty()) {
			return Resolve(offset, count);
		}
		// The filesystem bearer token and filesystem ETag MUST NOT cross this boundary.
		auto r = http.Request(object_url, "", false, headers, count);
		if (r.status >= 200 && r.status < 300) {
			auto tag = r.Header("etag");
			std::lock_guard<std::mutex> lock(version->mu);
			if (tag.empty() || tag.compare(0, 2, "W/") == 0 || (!version->object_etag.empty() && tag != version->object_etag)) {
				throw IOException("tidbfs backing object lacks a stable version");
			}
			version->object_etag = tag;
		}
		return r;
	}
	string Fetch(idx_t offset, idx_t count) {
		Response r;
		bool resolved;
		{
			std::lock_guard<std::mutex> lock(version->mu);
			resolved = !version->object_url.empty();
		}
		if (!resolved) {
			r = Resolve(offset, count);
		} else {
			r = ReadObject(offset, count);
			if (r.status == 401 || r.status == 403) {
				// One bounded refresh, with the original filesystem If-Match.
				{
					std::lock_guard<std::mutex> lock(version->mu);
					version->object_url.clear();
				}
				r = Resolve(offset, count);
			}
		}
		CheckStatus(r);
		auto expected = "bytes " + std::to_string(offset) + "-" + std::to_string(offset + count - 1) + "/" + std::to_string(size);
		if (r.status != 206 || r.Header("content-range") != expected || r.body.size() != count ||
		    (!r.Header("content-encoding").empty() && r.Header("content-encoding") != "identity")) {
			throw IOException("tidbfs expected an exact, uncompressed byte-range response");
		}
		return std::move(r.body);
	}
	void ReadAt(void *buffer, idx_t count, idx_t offset) {
		if (closed || offset > size || count > size - offset) {
			throw IOException("tidbfs read outside an open file");
		}
		if (spool) {
			spool->IO(buffer, count, offset, false);
			return;
		}
		auto out = static_cast<char *>(buffer);
		while (count) {
			if (offset >= cache_start && offset - cache_start < cache.size()) {
				auto available = MinValue<idx_t>(count, cache.size() - (offset - cache_start));
				memcpy(out, cache.data() + offset - cache_start, available);
				offset += available;
				out += available;
				count -= available;
				continue;
			}
			auto amount = MinValue<idx_t>(count, MAX_READ);
			auto begin = offset;
			auto end = offset + amount;
			if (config.read_ahead && amount < config.read_ahead) {
				// A tail window fetches the Parquet trailer and footer together.
				begin = offset >= size - MinValue(size, config.read_ahead)
				            ? size - MinValue(size, config.read_ahead)
				            : offset / config.read_ahead * config.read_ahead;
				end = MinValue(size, MaxValue(end, begin + config.read_ahead));
			}
			auto bytes = Fetch(begin, end - begin);
			memcpy(out, bytes.data() + offset - begin, amount);
			if (bytes.size() <= MAX_CACHE) {
				cache_start = begin;
				cache = std::move(bytes);
			}
			offset += amount;
			out += amount;
			count -= amount;
		}
	}
};

#include "extent_filesystem.hpp"

class TiDBFileSystem : public FileSystem {
	TiDBDatabaseFileSystem extent_fs;
public:
	explicit TiDBFileSystem(std::shared_ptr<ExtentClients> clients) : extent_fs(std::move(clients)) { }
	string GetName() const override { return "tidbfs"; }
	bool CanHandleFile(const string &path) override { return NormalizeURI(path).compare(0, 9, "tidbfs://") == 0; }
	bool CanSeek() override { return true; }
	bool OnDiskFile(FileHandle &) override { return false; }
	bool IsManuallySet() override { return true; }
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override {
		Path(path);
		// COPY requests WRITE_LOCK on write-only output handles. Those publish
		// a complete generation with revision CAS. Database handles also request
		// read access and require a real lifetime lock, which we must not fake.
		if (flags.Lock() != FileLockType::NO_LOCK && (!flags.OpenForWriting() || flags.OpenForReading())) {
			throw NotImplementedException("tidbfs cannot provide database file locks; use a local DuckDB database and remote COPY/EXPORT");
		}
		if (flags.CreatePrivateFile()) {
			throw NotImplementedException("tidbfs private file permissions are controlled by server credentials");
		}
		if (flags.OpenForWriting()) {
			return OpenWrite(path, flags, opener);
		}
		if (!flags.OpenForReading()) {
			throw InvalidInputException("tidbfs requires read or write access");
		}
		auto config = ReadConfig(opener, path);
		auto version = make_shared_ptr<FileVersion>();
		if (auto context = FileOpener::TryGetClientContext(opener)) {
			version = context->registered_state->GetOrCreate<QueryFiles>("tidbfs.query_files")->Get(config, NormalizeURI(path));
		}
		auto handle = make_uniq<TiDBHandle>(*this, path, flags, std::move(config), version);
		std::unique_lock<std::mutex> lock(version->mu);
		if (version->extent) {
			lock.unlock();
			return extent_fs.OpenFile(path, flags, opener);
		}
		if (version->initialized) {
			handle->size = version->size;
			handle->mtime = version->mtime;
			handle->etag = version->etag;
			return handle;
		}
		auto r = handle->http.Request(handle->Endpoint(), handle->config.token, true, {}, 0);
		if (r.status == 404 && flags.ReturnNullIfNotExists()) {
			return nullptr;
		}
		CheckStatus(r);
		if (r.Header("x-dat9-isdir") == "true") {
			throw IOException("tidbfs cannot open a directory as a file");
		}
		if (r.Header("x-dat9-content-layout") == "extent") {
			lock.unlock();
			return extent_fs.OpenFile(path, flags, opener);
		}
		handle->size = idx_t(Number(r.Header("content-length")));
		handle->etag = r.Header("etag");
		if (handle->etag.empty() || handle->etag.compare(0, 2, "W/") == 0 || r.Header("accept-ranges") != "bytes") {
			throw IOException("Upgrade fs: tidbfs requires strong ETags and byte-range reads");
		}
		if (!r.Header("x-dat9-mtime").empty()) {
			handle->mtime = Number(r.Header("x-dat9-mtime"));
		}
		version->size = handle->size;
		version->mtime = handle->mtime;
		version->etag = handle->etag;
		version->initialized = true;
		return handle;
	}
	unique_ptr<FileHandle> OpenWrite(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) {
		auto context = FileOpener::TryGetClientContext(opener);
		if (!context) {
			throw IOException("tidbfs writes require a DuckDB query context");
		}
		auto handle = make_uniq<TiDBHandle>(*this, path, flags, ReadConfig(opener, path), make_shared_ptr<FileVersion>());
		auto &h = *handle;
		h.deferred = context->registered_state->GetOrCreate<DeferredWrites>("tidbfs.pending_writes");
		// Reserve cleanup protection before I/O: OpenFile itself can fail
		// after COPY observed absence and another publisher created this path.
		h.deferred->Preserve(h.config, path);
		auto r = h.http.Request(h.Endpoint(), h.config.token, true, {}, 0);
		bool exists = r.status != 404;
		if (exists) {
			CheckStatus(r);
			if (flags.ReturnNullIfExists()) {
				return nullptr;
			}
			if (flags.ExclusiveCreate()) {
				throw IOException("tidbfs file already exists");
			}
			if (r.Header("x-dat9-isdir") == "true") {
				throw IOException("tidbfs cannot write a directory as a file");
			}
			if (r.Header("x-dat9-content-layout") == "extent")
				return extent_fs.OpenFile(path, flags | FileLockType::WRITE_LOCK, opener);
			if (r.Header("x-dat9-content-layout") != "single" && !r.Header("x-dat9-content-layout").empty()) {
				throw NotImplementedException("tidbfs writes require single-file content layout");
			}
			h.revision = Number(r.Header("x-dat9-revision"));
			if (h.revision <= 0 || h.revision == std::numeric_limits<int64_t>::max()) {
				throw IOException("tidbfs writes require a valid server revision");
			}
			h.size = idx_t(Number(r.Header("content-length")));
			h.etag = r.Header("etag");
		} else if (flags.ReturnNullIfNotExists()) {
			return nullptr;
		} else if (!flags.CreateFileIfNotExists() && !flags.OverwriteExistingFile()) {
			throw IOException("tidbfs file does not exist");
		}
		auto spool = make_uniq<Spool>();
		if (exists && !flags.OverwriteExistingFile()) {
			if (h.etag.empty() || h.etag.compare(0, 2, "W/") == 0) {
				throw IOException("tidbfs preserving writes require a strong file ETag");
			}
			for (idx_t offset = 0; offset < h.size;) {
				auto length = MinValue(MAX_READ, h.size - offset);
				auto data = h.Fetch(offset, length);
				spool->IO(&data[0], length, offset, true);
				offset += length;
			}
		} else {
			h.size = 0;
		}
		h.spool = std::move(spool);
		h.dirty = !exists || flags.OverwriteExistingFile();
		h.position = flags.OpenForAppending() ? h.size : 0;
		return handle;
	}
	void Write(FileHandle &base, void *buffer, int64_t count, idx_t offset) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		if (count < 0) {
			throw IOException("Negative tidbfs write size");
		}
		h.WriteAt(buffer, idx_t(count), offset);
	}
	int64_t Write(FileHandle &base, void *buffer, int64_t count) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		if (count < 0) {
			throw IOException("Negative tidbfs write size");
		}
		auto offset = h.flags.OpenForAppending() ? h.size : h.position;
		h.WriteAt(buffer, idx_t(count), offset);
		h.position = offset + idx_t(count);
		return count;
	}
	void FileSync(FileHandle &base) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		h.Sync();
	}
	void AbortFileWrite(FileHandle &base) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		// DuckDB 2.0 aborts asynchronous writers through this hook. Close would
		// publish partial staging, and generic COPY cleanup could then delete
		// an unrelated winner after a revision conflict or lost acknowledgement.
		h.deferred->Preserve(h.config, h.path);
		h.failed = true;
		h.closed = true;
		h.dirty = false;
		h.spool.reset();
	}
	void Truncate(FileHandle &base, int64_t size) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		if (!h.spool || h.closed || h.failed || size < 0) {
			throw IOException("Invalid tidbfs truncate");
		}
		if (ftruncate(h.spool->FD(), off_t(size)) != 0) {
			h.failed = true;
			throw IOException("tidbfs local truncate failed");
		}
		h.size = idx_t(size);
		h.dirty = true;
	}
	void Read(FileHandle &base, void *buffer, int64_t count, idx_t offset) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		if (count < 0) {
			throw IOException("Negative tidbfs read size");
		}
		h.ReadAt(buffer, idx_t(count), offset);
	}
	int64_t Read(FileHandle &base, void *buffer, int64_t count) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		if (count < 0) {
			throw IOException("Negative tidbfs read size");
		}
		auto amount = MinValue<idx_t>(idx_t(count), h.size - MinValue(h.position, h.size));
		h.ReadAt(buffer, amount, MinValue(h.position, h.size));
		h.position += amount;
		return int64_t(amount);
	}
	int64_t GetFileSize(FileHandle &base) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		return int64_t(h.size);
	}
	timestamp_t GetLastModifiedTime(FileHandle &base) override {
		auto seconds = base.Cast<TiDBHandle>().mtime;
		if (seconds > std::numeric_limits<int64_t>::max() / 1000000) {
			throw IOException("Invalid tidbfs modification time");
		}
		return timestamp_t(seconds * 1000000);
	}
	string GetVersionTag(FileHandle &base) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		return h.spool ? "" : h.etag;
	}
	FileType GetFileType(FileHandle &) override { return FileType::FILE_TYPE_REGULAR; }
	void Seek(FileHandle &base, idx_t position) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		h.position = position;
	}
	idx_t SeekPosition(FileHandle &base) override {
		auto &h = base.Cast<TiDBHandle>();
		std::lock_guard<std::mutex> lock(h.mu);
		return h.position;
	}
	bool FileExists(const string &path, optional_ptr<FileOpener> opener) override {
		auto config = ReadConfig(opener, path);
		HTTP http;
		auto r = http.Request(config.endpoint + "/v1/fs" + EncodePath(Path(path)), config.token, true, {}, 0);
		if (r.status == 404) {
			return false;
		}
		CheckStatus(r);
		return r.Header("x-dat9-isdir") != "true";
	}
	bool DirectoryExists(const string &path, optional_ptr<FileOpener> opener) override {
		auto directory = DirectoryPath(path);
		auto config = ReadConfig(opener, "tidbfs://" + directory);
		HTTP http;
		auto r = http.Request(config.endpoint + "/v1/fs" + EncodePath(directory), config.token, true, {}, 0);
		if (r.status == 404) {
			return false;
		}
		CheckStatus(r);
		return r.Header("x-dat9-isdir") == "true";
	}
	void CreateDirectory(const string &path, optional_ptr<FileOpener> opener) override {
		auto directory = DirectoryPath(path);
		auto config = ReadConfig(opener, "tidbfs://" + directory);
		HTTP http;
		auto r = http.Request(config.endpoint + "/v1/fs" + EncodePath(directory) + "?mkdir", config.token,
		                      false, {}, 65536, "POST");
		if (r.status == 409 && DirectoryExists(path, opener)) {
			return;
		}
		CheckStatus(r);
	}
	bool ListFiles(const string &path, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener) override {
		auto directory = DirectoryPath(path);
		auto config = ReadConfig(opener, "tidbfs://" + directory);
		HTTP http;
		auto r = http.Request(config.endpoint + "/v1/fs" + EncodePath(directory) + "?list=1", config.token,
		                      false, {}, MAX_LIST);
		if (r.status == 404) {
			return false;
		}
		auto json = JSON(r);
		if (!json.contains("entries") || !json["entries"].is_array()) {
			throw IOException("Invalid tidbfs directory response");
		}
		for (auto &entry : json["entries"]) {
			auto name = JSONString(entry, "name");
			if (!entry.contains("isDir") || !entry["isDir"].is_boolean()) {
				throw IOException("Invalid tidbfs directory entry type");
			}
			bool dir = entry["isDir"].get<bool>();
			if (dir && !name.empty() && name.back() == '/') {
				name.pop_back();
			}
			if (name.empty() || name == "." || name == ".." || name.find_first_of("/\\") != string::npos ||
			    name.find('\0') != string::npos) {
				throw IOException("Invalid tidbfs directory entry name");
			}
			callback(name, dir);
		}
		return true;
	}
	bool TryRemoveFile(const string &path, optional_ptr<FileOpener> opener) override {
		auto config = ReadConfig(opener, path);
		if (auto context = FileOpener::TryGetClientContext(opener)) {
			auto writes = context->registered_state->Get<DeferredWrites>("tidbfs.pending_writes");
			if (writes && writes->PreserveOnCleanup(config, path)) {
				return false;
			}
		}
		HTTP http;
		auto r = http.Request(config.endpoint + "/v1/fs" + EncodePath(Path(path)) + "?kind=file", config.token,
		                      false, {}, 65536, "DELETE");
		if (r.status == 404) {
			return false;
		}
		CheckStatus(r);
		return true;
	}
	void RemoveFile(const string &path, optional_ptr<FileOpener> opener) override {
		if (!TryRemoveFile(path, opener)) {
			throw IOException("tidbfs file does not exist");
		}
	}
	void RemoveDirectory(const string &path, optional_ptr<FileOpener> opener) override {
		auto directory = DirectoryPath(path);
		if (directory == "/") {
			throw IOException("tidbfs cannot recursively remove the filesystem root");
		}
		auto config = ReadConfig(opener, "tidbfs://" + directory);
		HTTP http;
		CheckStatus(http.Request(config.endpoint + "/v1/fs" + EncodePath(directory) + "?recursive", config.token,
		                         false, {}, 65536, "DELETE"));
	}
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		auto source_path = Path(source);
		auto target_path = Path(target);
		auto config = ReadConfig(opener, source);
		auto target_config = ReadConfig(opener, target);
		if (config.endpoint != target_config.endpoint || config.token != target_config.token) {
			throw IOException("tidbfs cannot atomically move files across credential scopes or filesystems");
		}
		HTTP http;
		CheckStatus(http.Request(config.endpoint + "/v1/fs" + EncodePath(target_path) + "?rename", config.token, false,
		                         {"X-Dat9-Path-Encoding: base64url", "X-Dat9-Rename-Source: " + SourceHeader(source_path)},
		                         65536, "POST"));
	}
	vector<OpenFileInfo> Glob(const string &pattern, FileOpener *opener) override {
		auto path = Path(pattern);
		if (!HasGlob(path)) {
			return {OpenFileInfo(pattern)};
		}
		auto parts = StringUtil::Split(path.substr(1), '/');
		HTTP http;
		std::set<string> matches;
		idx_t visited = 0;
		std::function<void(string, idx_t, idx_t)> walk = [&](string directory, idx_t part, idx_t depth) {
			if (depth > 64 || ++visited > 100000) {
				throw IOException("tidbfs glob traversal limit exceeded; use a narrower prefix");
			}
			if (part >= parts.size()) {
				return;
			}
			if (part + 1 < parts.size() && !HasGlob(parts[part])) {
				walk(directory + parts[part] + "/", part + 1, depth + 1);
				return;
			}
			auto config = ReadConfig(opener, "tidbfs://" + directory);
			auto r = http.Request(config.endpoint + "/v1/fs" + EncodePath(directory) + "?list=1", config.token, false, {}, MAX_LIST);
			if (r.status == 404) {
				return;
			}
			CheckStatus(r);
			auto json = nlohmann::json::parse(r.body, nullptr, false);
			if (json.is_discarded() || !json.contains("entries") || !json["entries"].is_array()) {
				throw IOException("Invalid tidbfs directory response");
			}
			bool recursive = parts[part] == "**";
			if (recursive && part + 1 < parts.size()) {
				walk(directory, part + 1, depth);
			}
			for (auto &entry : json["entries"]) {
				if (!entry.contains("name") || !entry["name"].is_string() || !entry.contains("isDir") || !entry["isDir"].is_boolean()) {
					throw IOException("Invalid tidbfs directory entry");
				}
				auto name = entry["name"].get<std::string>();
				bool dir = entry["isDir"].get<bool>();
				if (dir && !name.empty() && name.back() == '/') {
					name.pop_back();
				}
				if (name.empty() || name == "." || name == ".." || name.find_first_of("/\\") != string::npos || name.find('\0') != string::npos) {
					throw IOException("Invalid tidbfs directory entry name");
				}
				if (!recursive && fnmatch(parts[part].c_str(), name.c_str(), 0) != 0) {
					continue;
				}
				if (dir && (recursive || part + 1 < parts.size())) {
					walk(directory + name + "/", recursive ? part : part + 1, depth + 1);
				} else if (!dir && part + 1 == parts.size()) {
					matches.insert("tidbfs://" + directory + name);
					if (matches.size() > 100000) {
						throw IOException("tidbfs glob file limit exceeded");
					}
				}
			}
		};
		walk("/", 0, 0);
		vector<OpenFileInfo> result;
		for (auto &match : matches) {
			result.emplace_back(match);
		}
		return result;
	}
};



unique_ptr<BaseSecret> CreateTiDBSecret(ClientContext &, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.push_back("tidbfs://");
		scope.push_back("tidbfs+db://");
	}
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	if (!secret->TrySetValue("endpoint", input) || !secret->TrySetValue("token", input)) {
		throw InvalidInputException("tidbfs secret requires ENDPOINT and TOKEN");
	}
	CheckEndpoint(secret->secret_map["endpoint"].GetValue<string>());
	secret->redact_keys.insert("token");
	return secret;
}

} // namespace

void LoadTiDBFS(ExtensionLoader &loader) {
	static std::once_flag init;
	std::call_once(init, [] {
		if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
			throw IOException("Cannot initialize tidbfs HTTP library");
		}
	});
	SecretType type;
	type.name = "tidbfs";
	type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	type.default_provider = "config";
	type.extension = "tidbfs";
	loader.RegisterSecretType(type);
	CreateSecretFunction function;
	function.secret_type = "tidbfs";
	function.provider = "config";
	function.function = CreateTiDBSecret;
	function.named_parameters["endpoint"] = LogicalType::VARCHAR;
	function.named_parameters["token"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);
	DBConfig::GetConfig(loader.GetDatabaseInstance()).AddExtensionOption(
	    "tidbfs_read_ahead", "Bytes to fetch ahead on small reads (0 disables prefetch, max 1 MiB)",
	    LogicalType::BIGINT, Value::BIGINT(65536));
	DBConfig::GetConfig(loader.GetDatabaseInstance()).AddExtensionOption(
	    "tidbfs_heartbeat_ms", "Extent session heartbeat in milliseconds; set before first extent access (expiry = 5x)",
	    LogicalType::BIGINT, Value::BIGINT(12000),
	    [](ClientContext &, SetScope scope, Value &value) {
		    // Database/WAL opens also use an instance-level file opener. A
		    // session-local value would disagree with that opener in one ATTACH.
		    if (scope != SetScope::GLOBAL)
			    throw InvalidInputException("tidbfs_heartbeat_ms is global; use SET GLOBAL");
		    if (value.IsNull() || value.GetValue<int64_t>() < 1000 || value.GetValue<int64_t>() > 600000)
			    throw InvalidInputException("tidbfs_heartbeat_ms must be between 1000 and 600000");
	    }, SetScope::GLOBAL);
	auto clients = std::make_shared<ExtentClients>();
	FileSystem::GetFileSystem(loader.GetDatabaseInstance()).RegisterSubSystem(make_uniq<TiDBFileSystem>(clients));
	FileSystem::GetFileSystem(loader.GetDatabaseInstance()).RegisterSubSystem(make_uniq<TiDBDatabaseFileSystem>(clients));
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(tidbfs, loader) {
	duckdb::LoadTiDBFS(loader);
}
}
