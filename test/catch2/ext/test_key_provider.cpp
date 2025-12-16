/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ext/test/key_provider/key_provider.h"

#include "wrappers/connection_wrapper.h"
#include "utils.h"

#include "wiredtiger.h"
#include "wt_internal.h"

#include <catch2/catch.hpp>

#include <functional>
#include <iostream>
#include <memory>

/*
 * kp_fixture
 *     Test fixture for the key provider extension tests.
 */
struct kp_fixture {
    utils::shared_library lib{KEY_PROVIDER_EXTENSION};

    using extension_init_t = decltype(&wiredtiger_extension_init);
    extension_init_t extension_init = nullptr;

    connection_wrapper conn;
    WT_SESSION *session = nullptr;

    KEY_PROVIDER *kp = nullptr;
    using kp_ptr_t = std::unique_ptr<KEY_PROVIDER, std::function<void(KEY_PROVIDER *)>>;

    ~kp_fixture()
    {
        kp_reset();
    }

    kp_fixture()
        : extension_init(lib.get<extension_init_t>("wiredtiger_extension_init")),
          conn(DB_HOME, "create,in_memory")
    {
        REQUIRE(conn.get_wt_connection()->open_session(conn.get_wt_connection(), NULL, NULL, &session) == 0);
    }

    kp_ptr_t
    kp_init(const char *config)
    {
        const char *ext_config[] = {config, nullptr};

        int ret =
          extension_init(conn.get_wt_connection(), reinterpret_cast<WT_CONFIG_ARG *>(ext_config));
        REQUIRE(ret == 0);

        REQUIRE(kp == nullptr);
        kp = reinterpret_cast<KEY_PROVIDER *>(conn.get_wt_connection_impl()->key_provider);
        REQUIRE(kp != nullptr);

        return kp_ptr_t(kp, [this](KEY_PROVIDER *k) {
            assert(k == this->kp);
            this->kp_reset();
        });
    }

    void
    kp_reset()
    {
        if (kp != nullptr) {
            int ret = kp->iface.terminate(&kp->iface, session);
            if (ret != 0) {
                WARN("Error terminating key provider: " << ret);
            }
            kp = nullptr;
        }
        conn.get_wt_connection_impl()->key_provider = nullptr;
    }

    void
    kp_load_key(const std::string &key_data, uint64_t lsn)
    {
        REQUIRE(kp != nullptr);

        WT_CRYPT_KEYS crypt = {};
        crypt.r.lsn = lsn;
        crypt.keys.data = key_data.data();
        crypt.keys.size = key_data.size();

        int ret = kp->iface.load_key(&kp->iface, session, &crypt);
        REQUIRE(ret == 0);

        REQUIRE(kp->state.current_lsn == lsn);
        REQUIRE(memcmp(kp->state.current_key, key_data.data(), key_data.size()) == 0);
        REQUIRE(kp->state.key_size == key_data.size());
        REQUIRE(kp->state.key_time != 0);
    }

    WT_CRYPT_KEYS
    kp_get_key()
    {
        WT_KEY_PROVIDER *wtkp = &kp->iface;

        /* Get key; first query the size, then get the data */
        WT_CRYPT_KEYS crypt = {};
        REQUIRE(wtkp->get_key(wtkp, session, &crypt) == 0);

        REQUIRE(crypt.keys.size != 0); /* Key has changed */
        REQUIRE(crypt.keys.data == nullptr);
        REQUIRE(kp->state.current_lsn == 0); /* New key is not persisted yet */

        crypt.keys.data = malloc(crypt.keys.size);
        REQUIRE(wtkp->get_key(wtkp, session, &crypt) == 0);

        REQUIRE(crypt.keys.size == kp->state.key_size);
        REQUIRE(memcmp(crypt.keys.data, kp->state.current_key, crypt.keys.size) == 0);
        REQUIRE(kp->state.current_lsn == 0); /* New key is not persisted yet */

        return (crypt);
    }
};

TEST_CASE_METHOD(kp_fixture, "Config", "[key_provider]")
{
    SECTION("Null config")
    {
        kp_ptr_t kp = kp_init(nullptr);
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_INFO);
        REQUIRE(kp->key_expires == 0);
    }

    SECTION("Empty config")
    {
        kp_ptr_t kp = kp_init("");
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_INFO);
        REQUIRE(kp->key_expires == 0);
    }

    SECTION("Custom config")
    {
        kp_ptr_t kp = kp_init("verbose=2,key_expires=300");
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_DEBUG_2);
        REQUIRE(kp->key_expires == 300);
        REQUIRE(kp->state.current_lsn == 0);
        REQUIRE(kp->state.key_time == 0);
        REQUIRE(kp->state.current_key == nullptr);
        REQUIRE(kp->state.key_size == 0);
    }

    SECTION("Invalid config")
    {
        const char *invalid_configs[] = {
          "verbose=invalid",
          "key_expires=invalid",
          "verb=0,key=1",
          "wrong=bad",
        };

        for (const char *config : invalid_configs) {
            const char *ext_config[] = {config, nullptr};
            int ret = extension_init(conn.get_wt_connection(),
              reinterpret_cast<WT_CONFIG_ARG *>(ext_config));
            REQUIRE(ret == EINVAL);
        }
    }
}

TEST_CASE_METHOD(kp_fixture, "Key never expires", "[key_provider]")
{
    /* By default, the key never expires. */
    kp_ptr_t kp = kp_init(nullptr);
    REQUIRE(kp->wtext != nullptr);

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    /* Initial state */
    REQUIRE(kp->verbose == 0);
    REQUIRE(kp->key_expires == 0);
    REQUIRE(kp->state.current_lsn == 0);
    REQUIRE(kp->state.key_time == 0);
    REQUIRE(kp->state.current_key == nullptr);
    REQUIRE(kp->state.key_size == 0);

    /* Dummy key and LSN */
    const std::string dummy_key = "dummy_key_data";
    const uint64_t dummy_lsn = 42;

    kp_load_key(dummy_key, dummy_lsn);

    /* Probe the key; the key never expires */
    WT_CRYPT_KEYS crypt_out = {};
    REQUIRE(wtkp->get_key(wtkp, session, &crypt_out) == 0);
    REQUIRE(crypt_out.r.lsn == 0);
    REQUIRE(crypt_out.keys.size == 0); /* Key has not changed */
    REQUIRE(crypt_out.keys.data == nullptr);
}

TEST_CASE_METHOD(kp_fixture, "Key expire", "[key_provider]")
{
    /* Key expiration period = 12 hours */
    kp_ptr_t kp = kp_init("verbose=1,key_expires=43200");
    REQUIRE(kp->wtext != nullptr);

    REQUIRE(kp->verbose == WT_VERBOSE_DEBUG_1);
    REQUIRE(kp->key_expires == 43200);
    REQUIRE(kp->state.current_lsn == 0);
    REQUIRE(kp->state.key_time == 0);
    REQUIRE(kp->state.current_key == nullptr);
    REQUIRE(kp->state.key_size == 0);

    /* Dummy key and LSN */
    const std::string dummy_key = "dummy_key_data";
    const uint64_t dummy_lsn = 42;

    kp_load_key(dummy_key, dummy_lsn);

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    /* Key is not expired yet */
    WT_CRYPT_KEYS crypt = {};
    crypt.keys.data = nullptr; /* Indicate request for key size */
    crypt.keys.size = 123;     /* Arbitrary non-zero size (just for test) */
    REQUIRE(wtkp->get_key(wtkp, session, &crypt) == 0);

    REQUIRE(crypt.keys.size == 0); /* Key has not changed */

    /* Expire the key by setting the key_time to the past */
    kp->state.key_time -= (kp->key_expires + 1) * CLOCKS_PER_SEC;

    crypt = kp_get_key();
    free(const_cast<void *>(crypt.keys.data));
}

TEST_CASE_METHOD(kp_fixture, "Persist key, success", "[key_provider]")
{
    kp_ptr_t kp = kp_init("key_expires=43200");
    REQUIRE(kp->wtext != nullptr);

    REQUIRE(kp->verbose == WT_VERBOSE_INFO);
    REQUIRE(kp->key_expires == 43200);
    REQUIRE(kp->state.current_lsn == 0);
    REQUIRE(kp->state.key_time == 0);
    REQUIRE(kp->state.current_key == nullptr);
    REQUIRE(kp->state.key_size == 0);

    /* Load initial key */
    const std::string initial_key = "initial_key_data";
    const uint64_t initial_lsn = 1;
    kp_load_key(initial_key, initial_lsn);

    /* Expire the key by setting the key_time to the past */
    kp->state.key_time -= (kp->key_expires + 1) * CLOCKS_PER_SEC;

    WT_CRYPT_KEYS crypt = kp_get_key();

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    const uint64_t new_lsn = 84; /* New LSN after persistence */
    crypt.r.lsn = new_lsn;
    REQUIRE(wtkp->on_key_update(wtkp, session, &crypt) == 0);
    REQUIRE(kp->state.current_lsn == new_lsn); /* LSN should be updated on success */

    free(const_cast<void *>(crypt.keys.data));
}

TEST_CASE_METHOD(kp_fixture, "Persist key, failure", "[key_provider]")
{
    kp_ptr_t kp = kp_init("key_expires=43200");
    REQUIRE(kp->wtext != nullptr);

    REQUIRE(kp->verbose == WT_VERBOSE_INFO);
    REQUIRE(kp->key_expires == 43200);
    REQUIRE(kp->state.current_lsn == 0);
    REQUIRE(kp->state.key_time == 0);
    REQUIRE(kp->state.current_key == nullptr);
    REQUIRE(kp->state.key_size == 0);

    /* Load initial key */
    const std::string initial_key = "initial_key_data";
    const uint64_t initial_lsn = 1;
    kp_load_key(initial_key, initial_lsn);

    /* Expire the key by setting the key_time to the past */
    kp->state.key_time -= (kp->key_expires + 1) * CLOCKS_PER_SEC;

    WT_CRYPT_KEYS crypt = kp_get_key();

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    /* Simulate key persistence failure */
    crypt.keys.size = 0; /* Indicate failure */
    crypt.r.error = EIO; /* I/O error */
    REQUIRE(wtkp->on_key_update(wtkp, session, &crypt) == 0);

    REQUIRE(kp->state.current_lsn == 0); /* LSN should not be updated on failure */
    free(const_cast<void *>(crypt.keys.data));
}
