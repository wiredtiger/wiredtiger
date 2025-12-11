/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "ext/test/key_provider/key_provider.h"

#include "wrappers/mock_session.h"
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

    static constexpr char conn_config[] = "create,in_memory";
    WT_CONNECTION *conn = nullptr;

    KEY_PROVIDER *kp = nullptr;
    using kp_ptr_t = std::unique_ptr<KEY_PROVIDER, std::function<void(KEY_PROVIDER *)>>;

    ~kp_fixture()
    {
        reset();

        if (conn != nullptr) {
            conn->close(conn, nullptr);
            conn = nullptr;
        }
    }

    kp_fixture() : extension_init(lib.get<extension_init_t>("wiredtiger_extension_init"))
    {
        int ret = wiredtiger_open(DB_HOME, nullptr, conn_config, &conn);
        REQUIRE(ret == 0);
        REQUIRE(conn != nullptr);
    }

    kp_ptr_t
    setup(const char *config)
    {
        const char *ext_config[] = {config, nullptr};

        int ret = extension_init(conn, reinterpret_cast<WT_CONFIG_ARG *>(ext_config));
        REQUIRE(ret == 0);

        REQUIRE(kp == nullptr);
        kp = reinterpret_cast<KEY_PROVIDER *>(wt_conn_impl()->key_provider);
        REQUIRE(kp != nullptr);

        return kp_ptr_t(kp, [this](KEY_PROVIDER *k) {
            assert(k == this->kp);
            this->reset();
        });
    }

    void
    reset()
    {
        if (kp != nullptr) {
            int ret = kp->iface.terminate(&kp->iface, session());
            if (ret != 0) {
                WARN("Error terminating key provider: " << ret);
            }
            kp = nullptr;
        }
        wt_conn_impl()->key_provider = nullptr;
    }

    WT_CONNECTION_IMPL *
    wt_conn_impl()
    {
        return reinterpret_cast<WT_CONNECTION_IMPL *>(conn);
    }

    WT_SESSION *
    session()
    {
        return &wt_conn_impl()->default_session->iface;
    }
};

TEST_CASE_METHOD(kp_fixture, "Config", "[key_provider]")
{
    SECTION("Null config")
    {
        kp_ptr_t kp = setup(nullptr);
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_INFO);
        REQUIRE(kp->key_expires == 0);
    }

    SECTION("Empty config")
    {
        kp_ptr_t kp = setup("");
        REQUIRE(kp->wtext != nullptr);

        REQUIRE(kp->verbose == WT_VERBOSE_INFO);
        REQUIRE(kp->key_expires == 0);
    }

    SECTION("Custom config")
    {
        kp_ptr_t kp = setup("verbose=2,key_expires=300");
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
            int ret = extension_init(conn, reinterpret_cast<WT_CONFIG_ARG *>(ext_config));
            REQUIRE(ret == EINVAL);
        }
    }
}

TEST_CASE_METHOD(kp_fixture, "Key never expires", "[key_provider]")
{
    /* By default, the key never expires. */
    kp_ptr_t kp = setup(nullptr);
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

    /* Load key */
    WT_CRYPT_KEYS crypt_in = {};
    crypt_in.r.lsn = dummy_lsn;
    crypt_in.keys.data = dummy_key.data();
    crypt_in.keys.size = dummy_key.size();

    REQUIRE(wtkp->load_key(wtkp, session(), &crypt_in) == 0);

    REQUIRE(kp->state.current_lsn == dummy_lsn);
    REQUIRE(memcmp(kp->state.current_key, dummy_key.data(), dummy_key.size()) == 0);
    REQUIRE(kp->state.key_size == dummy_key.size());
    REQUIRE(kp->state.key_time != 0);

    /* Probe the key; the key never expires */
    WT_CRYPT_KEYS crypt_out = {};
    REQUIRE(wtkp->get_key(wtkp, session(), &crypt_out) == 0);
    REQUIRE(crypt_out.r.lsn == 0);
    REQUIRE(crypt_out.keys.size == 0); /* Key has not changed */
    REQUIRE(crypt_out.keys.data == nullptr);
}

TEST_CASE_METHOD(kp_fixture, "Key expire", "[key_provider]")
{
    /* Key expiration period = 12 hours */
    kp_ptr_t kp = setup("verbose=1,key_expires=43200");
    REQUIRE(kp->wtext != nullptr);

    WT_KEY_PROVIDER *wtkp = &kp->iface;

    REQUIRE(kp->verbose == WT_VERBOSE_DEBUG_1);
    REQUIRE(kp->key_expires == 43200);
    REQUIRE(kp->state.current_lsn == 0);
    REQUIRE(kp->state.key_time == 0);
    REQUIRE(kp->state.current_key == nullptr);
    REQUIRE(kp->state.key_size == 0);

    /* Dummy key and LSN */
    const std::string dummy_key = "dummy_key_data";
    const uint64_t dummy_lsn = 42;

    /* Load key */
    WT_CRYPT_KEYS crypt = {};
    crypt.r.lsn = dummy_lsn;
    crypt.keys.data = dummy_key.data();
    crypt.keys.size = dummy_key.size();

    REQUIRE(wtkp->load_key(wtkp, session(), &crypt) == 0);

    REQUIRE(kp->state.current_lsn == dummy_lsn);
    REQUIRE(memcmp(kp->state.current_key, dummy_key.data(), dummy_key.size()) == 0);
    REQUIRE(kp->state.key_size == dummy_key.size());
    REQUIRE(kp->state.key_time != 0);

    /* Key is not expired yet */
    crypt.keys.data = nullptr; /* Indicate request for key size */
    crypt.keys.size = 123;     /* Arbitrary non-zero size (just for test) */
    REQUIRE(wtkp->get_key(wtkp, session(), &crypt) == 0);

    REQUIRE(crypt.keys.size == 0); /* Key has not changed */

    /* Expire the key by setting the key_time to the past */
    kp->state.key_time -= (kp->key_expires + 1) * CLOCKS_PER_SEC;

    /* Get key; first query the size, then get the data */
    memset(&crypt, 0, sizeof(crypt));
    REQUIRE(wtkp->get_key(wtkp, session(), &crypt) == 0);

    REQUIRE(crypt.keys.size != 0); /* Key has changed */
    REQUIRE(crypt.keys.data == nullptr);
    REQUIRE(kp->state.current_lsn == 0); /* New key is not persisted yet */

    crypt.keys.data = malloc(crypt.keys.size);
    REQUIRE(wtkp->get_key(wtkp, session(), &crypt) == 0);

    REQUIRE(crypt.keys.size == kp->state.key_size);
    REQUIRE(memcmp(crypt.keys.data, kp->state.current_key, crypt.keys.size) == 0);
    REQUIRE(kp->state.current_lsn == 0); /* New key is not persisted yet */

    const size_t key_size = crypt.keys.size; /* Preserve key size */

    /* Simulate key persistence failure */
    crypt.keys.size = 0; /* Indicate failure */
    crypt.r.error = EIO; /* I/O error */
    REQUIRE(wtkp->on_key_update(wtkp, session(), &crypt) == 0);

    REQUIRE(kp->state.current_lsn == 0); /* LSN should not be updated on failure */

    /* Simulate key persistence success */
    const uint64_t new_lsn = 84; /* New LSN after persistence */
    crypt.keys.size = key_size;  /* Valid size */
    crypt.r.lsn = new_lsn;
    REQUIRE(wtkp->on_key_update(wtkp, session(), &crypt) == 0);
    REQUIRE(kp->state.current_lsn == new_lsn); /* LSN should be updated on success */

    free(const_cast<void *>(crypt.keys.data));
}
