import time

import pytest
from valkey import ResponseError
from valkey_flash_test_case import ValkeyFlashTestCase


class TestFlashLPushRPush(ValkeyFlashTestCase):

    def test_lpush_creates_key_and_returns_length(self):
        result = self.client.execute_command("FLASH.LPUSH", "lp1", "a", "b", "c")
        assert result == 3

    def test_lpush_prepends_in_reverse_order(self):
        self.client.execute_command("FLASH.LPUSH", "lp2", "a", "b", "c")
        result = self.client.execute_command("FLASH.LRANGE", "lp2", "0", "-1")
        # LPUSH a b c → [c, b, a]
        assert result == [b"c", b"b", b"a"]

    def test_rpush_appends_in_order(self):
        self.client.execute_command("FLASH.RPUSH", "rp1", "a", "b", "c")
        result = self.client.execute_command("FLASH.LRANGE", "rp1", "0", "-1")
        assert result == [b"a", b"b", b"c"]

    def test_push_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LPUSH", "k")

    def test_push_wrongtype_error(self):
        self.client.execute_command("FLASH.HSET", "htkey", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LPUSH", "htkey", "val")

    def test_lpush_ex_sets_ttl(self):
        self.client.execute_command("FLASH.LPUSH", "lpex", "v", "EX", "60")
        ttl = self.client.execute_command("PTTL", "lpex")
        assert 0 < ttl <= 60_000 + 1000

    def test_rpush_keepttl_preserves_ttl(self):
        self.client.execute_command("FLASH.RPUSH", "rp_kttl", "v1", "PX", "30000")
        ttl_before = self.client.execute_command("PTTL", "rp_kttl")
        self.client.execute_command("FLASH.RPUSH", "rp_kttl", "v2", "KEEPTTL")
        ttl_after = self.client.execute_command("PTTL", "rp_kttl")
        assert ttl_after > 0
        assert abs(ttl_before - ttl_after) < 1500

    def test_push_increments_length(self):
        self.client.execute_command("FLASH.RPUSH", "rp2", "x")
        self.client.execute_command("FLASH.RPUSH", "rp2", "y")
        assert self.client.execute_command("FLASH.LLEN", "rp2") == 2


class TestFlashLPushXRPushX(ValkeyFlashTestCase):

    def test_lpushx_returns_zero_for_missing_key(self):
        result = self.client.execute_command("FLASH.LPUSHX", "nokey", "v")
        assert result == 0

    def test_lpushx_pushes_to_existing_key(self):
        self.client.execute_command("FLASH.RPUSH", "lpxkey", "a")
        result = self.client.execute_command("FLASH.LPUSHX", "lpxkey", "b")
        assert result == 2
        items = self.client.execute_command("FLASH.LRANGE", "lpxkey", "0", "-1")
        assert items[0] == b"b"

    def test_rpushx_returns_zero_for_missing_key(self):
        result = self.client.execute_command("FLASH.RPUSHX", "nokey2", "v")
        assert result == 0

    def test_rpushx_pushes_to_existing_key(self):
        self.client.execute_command("FLASH.RPUSH", "rpxkey", "a")
        result = self.client.execute_command("FLASH.RPUSHX", "rpxkey", "b")
        assert result == 2

    def test_lpushx_wrongtype_error(self):
        self.client.execute_command("FLASH.HSET", "htkey2", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LPUSHX", "htkey2", "val")


class TestFlashLPopRPop(ValkeyFlashTestCase):

    def test_lpop_returns_head(self):
        self.client.execute_command("FLASH.RPUSH", "lpo1", "a", "b", "c")
        result = self.client.execute_command("FLASH.LPOP", "lpo1")
        assert result == b"a"

    def test_rpop_returns_tail(self):
        self.client.execute_command("FLASH.RPUSH", "rpo1", "a", "b", "c")
        result = self.client.execute_command("FLASH.RPOP", "rpo1")
        assert result == b"c"

    def test_lpop_missing_key_returns_nil(self):
        result = self.client.execute_command("FLASH.LPOP", "nokey")
        assert result is None

    def test_pop_with_count(self):
        self.client.execute_command("FLASH.RPUSH", "poc1", "a", "b", "c", "d")
        result = self.client.execute_command("FLASH.LPOP", "poc1", "2")
        assert result == [b"a", b"b"]

    def test_pop_deletes_key_when_empty(self):
        self.client.execute_command("FLASH.RPUSH", "podel", "only")
        self.client.execute_command("FLASH.LPOP", "podel")
        assert self.client.execute_command("EXISTS", "podel") == 0

    def test_pop_wrongtype_error(self):
        self.client.execute_command("FLASH.HSET", "ht_pop", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LPOP", "ht_pop")

    def test_pop_count_zero_returns_empty_array(self):
        self.client.execute_command("FLASH.RPUSH", "poc2", "a", "b")
        result = self.client.execute_command("FLASH.LPOP", "poc2", "0")
        assert result == []


class TestFlashLRange(ValkeyFlashTestCase):

    def test_lrange_full(self):
        self.client.execute_command("FLASH.RPUSH", "lr1", "a", "b", "c")
        result = self.client.execute_command("FLASH.LRANGE", "lr1", "0", "-1")
        assert result == [b"a", b"b", b"c"]

    def test_lrange_partial(self):
        self.client.execute_command("FLASH.RPUSH", "lr2", "a", "b", "c", "d")
        result = self.client.execute_command("FLASH.LRANGE", "lr2", "1", "2")
        assert result == [b"b", b"c"]

    def test_lrange_negative_indices(self):
        self.client.execute_command("FLASH.RPUSH", "lr3", "a", "b", "c")
        result = self.client.execute_command("FLASH.LRANGE", "lr3", "-2", "-1")
        assert result == [b"b", b"c"]

    def test_lrange_missing_key_returns_empty(self):
        result = self.client.execute_command("FLASH.LRANGE", "nokey", "0", "-1")
        assert result == []

    def test_lrange_out_of_range_returns_empty(self):
        self.client.execute_command("FLASH.RPUSH", "lr4", "a")
        result = self.client.execute_command("FLASH.LRANGE", "lr4", "5", "10")
        assert result == []

    def test_lrange_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LRANGE", "k")


class TestFlashLLen(ValkeyFlashTestCase):

    def test_llen_returns_count(self):
        self.client.execute_command("FLASH.RPUSH", "ll1", "a", "b", "c")
        assert self.client.execute_command("FLASH.LLEN", "ll1") == 3

    def test_llen_missing_key_returns_zero(self):
        assert self.client.execute_command("FLASH.LLEN", "nokey") == 0

    def test_llen_decrements_after_pop(self):
        self.client.execute_command("FLASH.RPUSH", "ll2", "a", "b")
        self.client.execute_command("FLASH.LPOP", "ll2")
        assert self.client.execute_command("FLASH.LLEN", "ll2") == 1

    def test_llen_wrongtype_error(self):
        self.client.execute_command("FLASH.HSET", "ht_ll", "f", "v")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LLEN", "ht_ll")


class TestFlashLIndex(ValkeyFlashTestCase):

    def test_lindex_positive(self):
        self.client.execute_command("FLASH.RPUSH", "li1", "a", "b", "c")
        assert self.client.execute_command("FLASH.LINDEX", "li1", "1") == b"b"

    def test_lindex_negative(self):
        self.client.execute_command("FLASH.RPUSH", "li2", "a", "b", "c")
        assert self.client.execute_command("FLASH.LINDEX", "li2", "-1") == b"c"

    def test_lindex_out_of_range_returns_nil(self):
        self.client.execute_command("FLASH.RPUSH", "li3", "a")
        assert self.client.execute_command("FLASH.LINDEX", "li3", "5") is None

    def test_lindex_missing_key_returns_nil(self):
        assert self.client.execute_command("FLASH.LINDEX", "nokey", "0") is None

    def test_lindex_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LINDEX", "k")


class TestFlashLSet(ValkeyFlashTestCase):

    def test_lset_replaces_element(self):
        self.client.execute_command("FLASH.RPUSH", "ls1", "a", "b", "c")
        self.client.execute_command("FLASH.LSET", "ls1", "1", "B")
        assert self.client.execute_command("FLASH.LINDEX", "ls1", "1") == b"B"

    def test_lset_negative_index(self):
        self.client.execute_command("FLASH.RPUSH", "ls2", "a", "b", "c")
        self.client.execute_command("FLASH.LSET", "ls2", "-1", "C")
        assert self.client.execute_command("FLASH.LINDEX", "ls2", "2") == b"C"

    def test_lset_out_of_range_error(self):
        self.client.execute_command("FLASH.RPUSH", "ls3", "a")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LSET", "ls3", "5", "v")

    def test_lset_missing_key_error(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LSET", "nokey", "0", "v")

    def test_lset_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LSET", "k", "0")


class TestFlashLInsert(ValkeyFlashTestCase):

    def test_linsert_before(self):
        self.client.execute_command("FLASH.RPUSH", "lin1", "a", "c")
        result = self.client.execute_command("FLASH.LINSERT", "lin1", "BEFORE", "c", "b")
        assert result == 3
        items = self.client.execute_command("FLASH.LRANGE", "lin1", "0", "-1")
        assert items == [b"a", b"b", b"c"]

    def test_linsert_after(self):
        self.client.execute_command("FLASH.RPUSH", "lin2", "a", "c")
        self.client.execute_command("FLASH.LINSERT", "lin2", "AFTER", "a", "b")
        items = self.client.execute_command("FLASH.LRANGE", "lin2", "0", "-1")
        assert items == [b"a", b"b", b"c"]

    def test_linsert_pivot_not_found_returns_negative_one(self):
        self.client.execute_command("FLASH.RPUSH", "lin3", "a", "b")
        result = self.client.execute_command("FLASH.LINSERT", "lin3", "BEFORE", "z", "v")
        assert result == -1

    def test_linsert_missing_key_returns_zero(self):
        result = self.client.execute_command("FLASH.LINSERT", "nokey", "BEFORE", "p", "v")
        assert result == 0

    def test_linsert_invalid_direction_error(self):
        self.client.execute_command("FLASH.RPUSH", "lin4", "a")
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LINSERT", "lin4", "MIDDLE", "a", "v")


class TestFlashLRem(ValkeyFlashTestCase):

    def test_lrem_removes_from_head(self):
        self.client.execute_command("FLASH.RPUSH", "lrem1", "a", "b", "a", "c", "a")
        result = self.client.execute_command("FLASH.LREM", "lrem1", "2", "a")
        assert result == 2
        items = self.client.execute_command("FLASH.LRANGE", "lrem1", "0", "-1")
        assert items == [b"b", b"c", b"a"]

    def test_lrem_removes_from_tail(self):
        self.client.execute_command("FLASH.RPUSH", "lrem2", "a", "b", "a")
        result = self.client.execute_command("FLASH.LREM", "lrem2", "-1", "a")
        assert result == 1
        items = self.client.execute_command("FLASH.LRANGE", "lrem2", "0", "-1")
        assert items == [b"a", b"b"]

    def test_lrem_removes_all_when_count_zero(self):
        self.client.execute_command("FLASH.RPUSH", "lrem3", "x", "x", "y", "x")
        result = self.client.execute_command("FLASH.LREM", "lrem3", "0", "x")
        assert result == 3
        assert self.client.execute_command("FLASH.LLEN", "lrem3") == 1

    def test_lrem_deletes_key_when_list_emptied(self):
        self.client.execute_command("FLASH.RPUSH", "lrem4", "a", "a")
        self.client.execute_command("FLASH.LREM", "lrem4", "0", "a")
        assert self.client.execute_command("EXISTS", "lrem4") == 0

    def test_lrem_missing_key_returns_zero(self):
        assert self.client.execute_command("FLASH.LREM", "nokey", "1", "v") == 0


class TestFlashLTrim(ValkeyFlashTestCase):

    def test_ltrim_keeps_middle(self):
        self.client.execute_command("FLASH.RPUSH", "lt1", "a", "b", "c", "d", "e")
        self.client.execute_command("FLASH.LTRIM", "lt1", "1", "3")
        items = self.client.execute_command("FLASH.LRANGE", "lt1", "0", "-1")
        assert items == [b"b", b"c", b"d"]

    def test_ltrim_out_of_range_deletes_key(self):
        self.client.execute_command("FLASH.RPUSH", "lt2", "a", "b")
        self.client.execute_command("FLASH.LTRIM", "lt2", "5", "10")
        assert self.client.execute_command("EXISTS", "lt2") == 0

    def test_ltrim_missing_key_returns_ok(self):
        result = self.client.execute_command("FLASH.LTRIM", "nokey", "0", "-1")
        assert result == b"OK"

    def test_ltrim_negative_indices(self):
        self.client.execute_command("FLASH.RPUSH", "lt3", "a", "b", "c")
        self.client.execute_command("FLASH.LTRIM", "lt3", "-2", "-1")
        items = self.client.execute_command("FLASH.LRANGE", "lt3", "0", "-1")
        assert items == [b"b", b"c"]


class TestFlashLMove(ValkeyFlashTestCase):

    def test_lmove_left_to_right(self):
        self.client.execute_command("FLASH.RPUSH", "lm_src", "a", "b", "c")
        self.client.execute_command("FLASH.RPUSH", "lm_dst", "x")
        elem = self.client.execute_command("FLASH.LMOVE", "lm_src", "lm_dst", "LEFT", "RIGHT")
        assert elem == b"a"
        assert self.client.execute_command("FLASH.LRANGE", "lm_src", "0", "-1") == [b"b", b"c"]
        dst = self.client.execute_command("FLASH.LRANGE", "lm_dst", "0", "-1")
        assert dst[-1] == b"a"

    def test_lmove_right_to_left(self):
        self.client.execute_command("FLASH.RPUSH", "lm2", "a", "b", "c")
        self.client.execute_command("FLASH.RPUSH", "lm2_dst", "x")
        elem = self.client.execute_command("FLASH.LMOVE", "lm2", "lm2_dst", "RIGHT", "LEFT")
        assert elem == b"c"
        dst = self.client.execute_command("FLASH.LRANGE", "lm2_dst", "0", "-1")
        assert dst[0] == b"c"

    def test_lmove_rotation(self):
        self.client.execute_command("FLASH.RPUSH", "rot", "a", "b", "c")
        elem = self.client.execute_command("FLASH.LMOVE", "rot", "rot", "LEFT", "RIGHT")
        assert elem == b"a"
        items = self.client.execute_command("FLASH.LRANGE", "rot", "0", "-1")
        assert items == [b"b", b"c", b"a"]

    def test_lmove_missing_src_returns_nil(self):
        result = self.client.execute_command("FLASH.LMOVE", "nokey", "dst", "LEFT", "RIGHT")
        assert result is None

    def test_lmove_creates_dst_if_absent(self):
        self.client.execute_command("FLASH.RPUSH", "lm3", "v")
        self.client.execute_command("FLASH.LMOVE", "lm3", "lm3_new", "LEFT", "LEFT")
        assert self.client.execute_command("FLASH.LLEN", "lm3_new") == 1

    def test_lmove_deletes_src_when_emptied(self):
        self.client.execute_command("FLASH.RPUSH", "lm4", "only")
        self.client.execute_command("FLASH.LMOVE", "lm4", "lm4_dst", "LEFT", "RIGHT")
        assert self.client.execute_command("EXISTS", "lm4") == 0

    def test_lmove_wrong_arity(self):
        with pytest.raises(ResponseError):
            self.client.execute_command("FLASH.LMOVE", "k", "d", "LEFT")


class TestFlashListColdTier(ValkeyFlashTestCase):

    def test_lrange_after_demote(self):
        self.client.execute_command("FLASH.RPUSH", "cold_lr", "a", "b", "c")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_lr")
        result = self.client.execute_command("FLASH.LRANGE", "cold_lr", "0", "-1")
        assert result == [b"a", b"b", b"c"]

    def test_llen_after_demote(self):
        self.client.execute_command("FLASH.RPUSH", "cold_ll", "x", "y")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_ll")
        assert self.client.execute_command("FLASH.LLEN", "cold_ll") == 2

    def test_lpush_after_demote_promotes_and_updates(self):
        self.client.execute_command("FLASH.RPUSH", "cold_lp", "a", "b")
        self.client.execute_command("FLASH.DEBUG.DEMOTE", "cold_lp")
        self.client.execute_command("FLASH.LPUSH", "cold_lp", "z")
        items = self.client.execute_command("FLASH.LRANGE", "cold_lp", "0", "-1")
        assert items[0] == b"z"
        assert len(items) == 3


class TestFlashListReplication(ValkeyFlashTestCase):

    def test_lpush_replicates_to_replica(self):
        from valkeytestframework.conftest import resource_port_tracker
        primary_port = self.server.port
        replica_server, replica_client = self.create_server(
            testdir=self.testdir,
            server_path=self.server.server_path,
            args={
                "enable-debug-command": "yes",
                "loadmodule": __import__("os").getenv("MODULE_PATH"),
                "replicaof": f"127.0.0.1 {primary_port}",
            },
        )
        try:
            import time
            time.sleep(0.5)
            self.client.execute_command("FLASH.RPUSH", "repl_list", "a", "b", "c")
            time.sleep(0.3)
            result = replica_client.execute_command("FLASH.LRANGE", "repl_list", "0", "-1")
            assert result == [b"a", b"b", b"c"]
        finally:
            replica_server.stop()
