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
        session->close(session, nullptr);
        session = nullptr;
    }

    kp_fixture()
        : extension_init(lib.get<extension_init_t>("wiredtiger_extension_init")),
          conn(DB_HOME, "create,in_memory")
    {
        REQUIRE(conn.get_wt_connection()->open_session(
                  conn.get_wt_connection(), NULL, NULL, &session) == 0);
        REQUIRE(session != nullptr);
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
        WT_KEY_PROVIDER *wtkp = &kp->iface;

        WT_CRYPT_KEYS crypt = {};
        crypt.r.lsn = lsn;
        crypt.keys.data = key_data.data();
        crypt.keys.size = key_data.size();

        int ret = wtkp->load_key(wtkp, session, &crypt);
        REQUIRE(ret == 0);

        REQUIRE(kp->state.current_lsn == lsn);
        REQUIRE(memcmp(kp->state.current_key, key_data.data(), key_data.size()) == 0);
        REQUIRE(kp->state.key_size == key_data.size());
        REQUIRE(kp->state.key_time != 0);
    }

    WT_CRYPT_KEYS
    kp_get_key()
    {
        REQUIRE(kp != nullptr);
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
        REQUIRE(kp->key_expires == 43200); /* Default: 12 hours */
    }

    SECTION("Empty config")
    {
        kp_ptr_t kp = kp_init("");
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_INFO);
        REQUIRE(kp->key_expires == 43200); /* Default: 12 hours */
    }

    SECTION("Custom config, regular key expiration")
    {
        kp_ptr_t kp = kp_init("verbose=2,key_expires=300");
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_DEBUG_2);
        REQUIRE(kp->key_expires == 300);
    }

    SECTION("Custom config, key never expires")
    {
        kp_ptr_t kp = kp_init("verbose=2,key_expires=-1");
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_DEBUG_2);
        REQUIRE(kp->key_expires == KEY_EXPIRES_NEVER);
    }

    SECTION("Custom config, key always expires")
    {
        kp_ptr_t kp = kp_init("verbose=2,key_expires=0");
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_DEBUG_2);
        REQUIRE(kp->key_expires == KEY_EXPIRES_ALWAYS);
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
            int ret = extension_init(
              conn->get_wt_connection(), reinterpret_cast<WT_CONFIG_ARG *>(ext_config));
            REQUIRE(ret == EINVAL);
        }
    }
}

TEST_CASE_METHOD(kp_fixture, "Default values", "[key_provider]")
{
    kp_ptr_t kp = kp_init(nullptr);
    REQUIRE(kp->wtext != nullptr);

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    /* Initial state */
    REQUIRE(kp->verbose == WT_VERBOSE_INFO);
    REQUIRE(kp->key_expires == 43200);
    REQUIRE(kp->state.current_lsn == 0);
    REQUIRE(kp->state.key_time == 0);
    REQUIRE(kp->state.current_key == nullptr);
    REQUIRE(kp->state.key_size == 0);
}

TEST_CASE_METHOD(kp_fixture, "Load key", "[key_provider]")
{
    kp_ptr_t kp = kp_init(nullptr);
    REQUIRE(kp->wtext != nullptr);

    /* Dummy key and LSN */
    const std::string dummy_key = "dummy_key_data";
    const uint64_t dummy_lsn = 42;

    kp_load_key(dummy_key, dummy_lsn);
}

TEST_CASE_METHOD(kp_fixture, "Key expired on first get_key call", "[key_provider][key_debug]")
{
    /* Key expiration period = 12 hours */
    kp_ptr_t kp = kp_init(nullptr);
    REQUIRE(kp->wtext != nullptr);

    REQUIRE(kp->verbose == WT_VERBOSE_INFO);
    REQUIRE(kp->key_expires == 43200);
    REQUIRE(kp->state.key_time == 0);

    WT_KEY_PROVIDER *wtkp = &kp->iface;
    const void *prev_key = kp->state.current_key;

    /* First call to get_key always generates a new key */
    WT_CRYPT_KEYS crypt = kp_get_key();
    REQUIRE(prev_key != kp->state.current_key); /* New key generated */
    REQUIRE(kp->state.current_lsn == 0);        /* New key is not persisted yet */
    free(const_cast<void *>(crypt.keys.data));

    /* Subsequent call to get_key should not generate a new key */
    crypt.keys.data = nullptr; /* Indicate request for key size */
    REQUIRE(wtkp->get_key(wtkp, session, &crypt) == 0);
    REQUIRE(crypt.keys.size == 0); /* Key has not changed */
    REQUIRE(crypt.keys.data == nullptr);
}

TEST_CASE_METHOD(kp_fixture, "Key expired and rotated", "[key_provider]")
{
    /* Key expiration period = 12 hours */
    kp_ptr_t kp = kp_init(nullptr);
    REQUIRE(kp->wtext != nullptr);

    REQUIRE(kp->verbose == WT_VERBOSE_INFO);
    REQUIRE(kp->key_expires == 43200);

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    /* First call to get always generates a new key */
    WT_CRYPT_KEYS crypt = kp_get_key();
    const clock_t first_key_time = kp->state.key_time;
    free(const_cast<void *>(crypt.keys.data));

    /* Expire the key by setting the key_time to the past */
    kp->state.key_time -= (kp->key_expires + 1) * CLOCKS_PER_SEC;

    /* Generate a new key */
    crypt = kp_get_key();
    REQUIRE(first_key_time != kp->state.key_time); /* New key generated */
    free(const_cast<void *>(crypt.keys.data));
}

TEST_CASE_METHOD(kp_fixture, "Persist key, success", "[key_provider]")
{
    kp_ptr_t kp = kp_init(nullptr);
    REQUIRE(kp->wtext != nullptr);

    /* First call to get_key always generates a new key */
    WT_CRYPT_KEYS crypt = kp_get_key();

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    const uint64_t new_lsn = 84; /* New LSN after persistence */
    crypt.r.lsn = new_lsn;
    REQUIRE(wtkp->on_key_update(wtkp, session, &crypt) == 0);
    /* LSN should be updated on success */
    REQUIRE(kp->state.current_lsn == new_lsn);

    free(const_cast<void *>(crypt.keys.data));
}

TEST_CASE_METHOD(kp_fixture, "Persist key, failure", "[key_provider]")
{
    kp_ptr_t kp = kp_init(nullptr);
    REQUIRE(kp->wtext != nullptr);

    /* First call to get_key always generates a new key */
    WT_CRYPT_KEYS crypt = kp_get_key();

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    /* Simulate key persistence failure */
    crypt.keys.size = 0; /* Indicate failure */
    crypt.r.error = EIO; /* I/O error */
    REQUIRE(wtkp->on_key_update(wtkp, session, &crypt) == 0);

    /* LSN should not be updated on failure */
    REQUIRE(kp->state.current_lsn == 0);

    free(const_cast<void *>(crypt.keys.data));
}

TEST_CASE_METHOD(kp_fixture, "Key never expires", "[key_provider]")
{
    /* Never expires */
    kp_ptr_t kp = kp_init("key_expires=-1");
    REQUIRE(kp->wtext != nullptr);

    REQUIRE(kp->verbose == WT_VERBOSE_INFO);
    REQUIRE(kp->key_expires == KEY_EXPIRES_NEVER);
    REQUIRE(kp->state.current_lsn == 0);
    REQUIRE(kp->state.key_time == 0);
    REQUIRE(kp->state.current_key == nullptr);

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    WT_CRYPT_KEYS crypt = {};
    REQUIRE(wtkp->get_key(wtkp, session, &crypt) == 0);
    REQUIRE(crypt.r.lsn == 0);
    REQUIRE(crypt.keys.size == 0); /* Key has not changed */
    REQUIRE(crypt.keys.data == nullptr);

    /* Simulate time passing */
    kp->state.key_time = 0;

    /* Probe the key again; the key has not expired */
    crypt.keys.data = nullptr; /* Indicate request for key size */
    REQUIRE(wtkp->get_key(wtkp, session, &crypt) == 0);
    REQUIRE(crypt.keys.size == 0); /* Key has not changed */
    REQUIRE(crypt.keys.data == nullptr);
}

TEST_CASE_METHOD(kp_fixture, "Key always expires", "[key_provider]")
{
    kp_ptr_t kp = kp_init("key_expires=0");
    REQUIRE(kp->wtext != nullptr);

    REQUIRE(kp->verbose == WT_VERBOSE_INFO);
    REQUIRE(kp->key_expires == KEY_EXPIRES_ALWAYS);

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    /* Probe the key; the key is always expired */
    WT_CRYPT_KEYS crypt = kp_get_key();
    const clock_t first_key_time = kp->state.key_time;
    REQUIRE(first_key_time != 0);

    /* Simulate persistence */
    const uint64_t new_lsn = 84; /* New LSN after persistence */
    crypt.r.lsn = new_lsn;
    REQUIRE(wtkp->on_key_update(wtkp, session, &crypt) == 0);
    /* LSN should be updated on success */
    REQUIRE(kp->state.current_lsn == new_lsn);
    free(const_cast<void *>(crypt.keys.data));

    /* Probe the key again; the key is always expired */
    crypt = kp_get_key();
    REQUIRE(first_key_time != kp->state.key_time); /* New key generated */
    /* New key is not persisted yet */
    REQUIRE(kp->state.current_lsn == 0);
    free(const_cast<void *>(crypt.keys.data));
}
