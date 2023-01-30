#include "wiredtiger.h"
#include "wiredtiger_ext.h"
#include <fstream>
#include <list>
#include <errno.h>
#include <filesystem>
#include <mutex>

#include "gcp_connection.h"
#include "wt_internal.h"

struct gcp_file_system;
struct gcp_file_handle;

/*
 * The first struct member must be the WT interface that is being implemented.
 */
struct gcp_store {
    WT_STORAGE_SOURCE store;
    WT_EXTENSION_API *wt_api; 
    std::shared_ptr<gcp_file_system> log;
    std::vector<gcp_file_system> gcp_fs;
    uint32_t reference_count;
};

struct gcp_file_system {
    WT_FILE_SYSTEM file_system;
    gcp_store *store;
    WT_FILE_SYSTEM *wt_file_system;
    std::vector<gcp_file_handle> gcp_fh;
    gcp_connection *gcp_conn;
    std::string home_dir;
};

struct gcp_file_handle {
    WT_FILE_HANDLE fh;
    gcp_store *store;
    WT_FILE_HANDLE wt_file_handle;
};

static int gcp_customize_file_system(WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *,
  const char *, WT_FILE_SYSTEM **) __attribute__((__unused__));
static int gcp_add_reference(WT_STORAGE_SOURCE *) __attribute__((__unused__));
static int gcp_file_system_terminate(WT_FILE_SYSTEM *, WT_SESSION *) __attribute__((__unused__));
static int gcp_flush(WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *,
  const char *, const char *) __attribute__((__unused__));
static int gcp_flush_finish(WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *,
  const char *, const char *) __attribute__((__unused__));
static int gcp_file_exists(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *)
  __attribute__((__unused__));
static int gcp_file_open(WT_FILE_SYSTEM *, WT_SESSION *, const char *, WT_FS_OPEN_FILE_TYPE,
  uint32_t, WT_FILE_HANDLE **) __attribute__((__unused__));
static int gcp_remove(WT_FILE_SYSTEM *, WT_SESSION *, const char *, uint32_t)
  __attribute__((__unused__));
static int gcp_rename(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, uint32_t)
  __attribute__((__unused__));
static int gcp_file_size(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *) __attribute__((__unused__));
static int gcp_object_list(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int gcp_object_list_add(const gcp_store &, char ***, const std::vector<std::string> &,
  const uint32_t) __attribute__((__unused__));
static int gcp_object_list_single(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *,
  char ***, uint32_t *) __attribute__((__unused__));
static int gcp_object_list_free(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t)
  __attribute__((__unused__));

static int
gcp_customize_file_system(WT_STORAGE_SOURCE *store, WT_SESSION *session, const char *bucket,
  const char *auth_token, const char *config, WT_FILE_SYSTEM **file_system)
{
    gcp_store *gcp;
    int ret;

    gcp = (gcp_store *)store;

    // Check if bucket name is given
    if (bucket == nullptr || strlen(bucket) == 0) {
      std::cerr << "GCPCustomizeFileSystem: bucket not specified." << std::endl;
      return (EINVAL);
    }



    // Fail if there is no authentication provided.
    if (auth_token == nullptr || strlen(auth_token) == 0) {
        std::cerr << "GCPCustomizeFileSystem: auth_token not specified." << std::endl;
        return (EINVAL);
    }

    // Get any prefix to be used for the object keys.
    WT_CONFIG_ITEM obj_prefix_conf;
    std::string obj_prefix;
    if ((ret = gcp->wt_api->config_get_string(
           gcp->wt_api, session, config, "prefix", &obj_prefix_conf)) == 0)
        obj_prefix = std::string(obj_prefix_conf.str, obj_prefix_conf.len);
    else if (ret != WT_NOTFOUND) {
        std::cerr << "GCPCustomizefs: error parsing config for object prefix." << std::endl;
        return (ret);
    }

    // Configure the GCP client configuration.
    // google::cloud::storage::ClientOptionList gcp_options;

    WT_FILE_SYSTEM *wt_file_system;
    if ((ret = gcp->wt_api->file_system_get(gcp->wt_api, session, &wt_file_system)) != 0)
        return (ret);

    // Get a copy of the home and cache directory.
    const std::string home_dir = session->connection->get_home(session->connection);

    // Create the file system.
    gcp_file_system *fs;
    if ((fs = (gcp_file_system *)calloc(1, sizeof(gcp_file_system))) == nullptr) {
        std::cerr << "GCPCustomizefs: unable to allocate memory for file system." << std::endl;
        return (ENOMEM);
    }

    fs->store = gcp;
    fs->wt_file_system = wt_file_system;
    fs->home_dir = home_dir;

    try {
        fs->gcp_conn = new gcp_connection(bucket, obj_prefix);
    } catch (std::invalid_argument &e) {
        std::cerr << std::string("GCPCustomizeFileSystem: ") + e.what() << std::endl;
        return (EINVAL);
    }

    fs->file_system.fs_directory_list = gcp_object_list;
    fs->file_system.fs_directory_list_single = gcp_object_list_single;
    fs->file_system.fs_directory_list_free = gcp_object_list_free;
    fs->file_system.terminate = gcp_file_system_terminate;
    fs->file_system.fs_exist = gcp_file_exists;
    fs->file_system.fs_open_file = gcp_file_open;
    fs->file_system.fs_remove = gcp_remove;
    fs->file_system.fs_rename = gcp_rename;
    fs->file_system.fs_size = gcp_file_size;

    *file_system = &fs->file_system;
    return 0;
}

static int
gcp_add_reference(WT_STORAGE_SOURCE *store)
{
    gcp_store *gcp = (gcp_store *)store;

    if (gcp->reference_count == 0 || gcp->reference_count + 1 == 0) {
        std::cerr << "GCPAddReference: missing reference or overflow." << std::endl;
        return (EINVAL);
    }

    ++gcp->reference_count;
    return (0);

    return 0;
}

static int
gcp_file_system_terminate(WT_FILE_SYSTEM *file_system, WT_SESSION *session)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);

    return 0;
}

static int
gcp_flush(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session, WT_FILE_SYSTEM *file_system,
  const char *source, const char *object, const char *config)
{
    WT_UNUSED(storage_source);
    WT_UNUSED(session);
    WT_UNUSED(file_system);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);

    return 0;
}

static int
gcp_flush_finish(WT_STORAGE_SOURCE *storage, WT_SESSION *session, WT_FILE_SYSTEM *file_system,
  const char *source, const char *object, const char *config)
{
    WT_UNUSED(storage);
    WT_UNUSED(session);
    WT_UNUSED(file_system);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);

    return 0;
}

static int
gcp_file_exists(
  WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, bool *file_exists)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(file_exists);

    return 0;
}

static int
gcp_file_open(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handle_ptr)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(file_type);
    WT_UNUSED(flags);
    WT_UNUSED(file_handle_ptr);

    return 0;
}

static int
gcp_remove(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(flags);

    return 0;
}

static int
gcp_rename(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *from, const char *to,
  uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(from);
    WT_UNUSED(to);
    WT_UNUSED(flags);

    return 0;
}

static int
gcp_file_size(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, wt_off_t *sizep)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(sizep);

    return 0;
}

static int gcp_object_list(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***object_list, uint32_t *count)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(directory);
    WT_UNUSED(prefix);
    WT_UNUSED(object_list);
    WT_UNUSED(count);

    return 0;
}


static int
gcp_object_list_add(const gcp_store &gcp_, char ***object_list,
  const std::vector<std::string> &objects, const uint32_t count)
{
    WT_UNUSED(gcp_);
    WT_UNUSED(object_list);
    WT_UNUSED(objects);
    WT_UNUSED(count);

    return 0;
}

static int
gcp_object_list_single(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***object_list, uint32_t *count)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(directory);
    WT_UNUSED(prefix);
    WT_UNUSED(object_list);
    WT_UNUSED(count);

    return 0;
}

static int
gcp_object_list_free(
  WT_FILE_SYSTEM *file_system, WT_SESSION *session, char **object_list, uint32_t count)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(object_list);
    WT_UNUSED(count);

    return 0;
}

int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    WT_UNUSED(connection);
    WT_UNUSED(config);

    return 0;
}
