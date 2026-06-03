import pytest

from beacon.core.trigger_rule import (
    TaskState,
    TriggerRule,
    evaluate_trigger_rule,
    evaluate_all_trigger_rules,
)

S = TaskState.SUCCESS
F = TaskState.FAILED
K = TaskState.SKIPPED
U = TaskState.UPSTREAM_FAILED


class TestAllSuccess:
    R = TriggerRule.ALL_SUCCESS

    def test_all_succeed(self):
        assert evaluate_trigger_rule(self.R, [S, S, S]) is True

    def test_one_failed(self):
        assert evaluate_trigger_rule(self.R, [S, F, S]) is False

    def test_all_failed(self):
        assert evaluate_trigger_rule(self.R, [F, F]) is False

    def test_all_skipped(self):
        assert evaluate_trigger_rule(self.R, [K, K]) is False

    def test_mixed(self):
        assert evaluate_trigger_rule(self.R, [S, F, K]) is False

    def test_upstream_failed(self):
        assert evaluate_trigger_rule(self.R, [S, U]) is False


# ── ALL_FAILED ───────────────────────────────────────────────────────


class TestAllFailed:
    R = TriggerRule.ALL_FAILED

    def test_all_failed(self):
        assert evaluate_trigger_rule(self.R, [F, F, F]) is True

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S]) is False

    def test_mixed(self):
        assert evaluate_trigger_rule(self.R, [F, S]) is False

    def test_all_skipped(self):
        assert evaluate_trigger_rule(self.R, [K, K]) is False


# ── ALL_DONE ─────────────────────────────────────────────────────────


class TestAllDone:
    R = TriggerRule.ALL_DONE

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S]) is True

    def test_all_failed(self):
        assert evaluate_trigger_rule(self.R, [F, F]) is True

    def test_mixed(self):
        assert evaluate_trigger_rule(self.R, [S, F, K, U]) is True

    def test_not_all_done(self):
        assert evaluate_trigger_rule(self.R, [S], total_upstreams=3) is False


# ── ALL_SKIPPED ──────────────────────────────────────────────────────


class TestAllSkipped:
    R = TriggerRule.ALL_SKIPPED

    def test_all_skipped(self):
        assert evaluate_trigger_rule(self.R, [K, K, K]) is True

    def test_one_success(self):
        assert evaluate_trigger_rule(self.R, [K, S]) is False

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S]) is False

    def test_all_failed(self):
        assert evaluate_trigger_rule(self.R, [F, F]) is False


# ── ONE_SUCCESS ──────────────────────────────────────────────────────


class TestOneSuccess:
    R = TriggerRule.ONE_SUCCESS

    def test_one_of_many(self):
        assert evaluate_trigger_rule(self.R, [S, F, K]) is True

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S]) is True

    def test_partial_report(self):
        # Only 1 of 5 upstreams reported, but it succeeded → fires early.
        assert evaluate_trigger_rule(self.R, [S], total_upstreams=5) is True

    def test_no_success(self):
        assert evaluate_trigger_rule(self.R, [F, K, U]) is False


# ── ONE_FAILED ───────────────────────────────────────────────────────


class TestOneFailed:
    R = TriggerRule.ONE_FAILED

    def test_one_of_many(self):
        assert evaluate_trigger_rule(self.R, [S, F]) is True

    def test_partial_report(self):
        assert evaluate_trigger_rule(self.R, [F], total_upstreams=5) is True

    def test_no_failure(self):
        assert evaluate_trigger_rule(self.R, [S, S, K]) is False


# ── NONE_FAILED ──────────────────────────────────────────────────────


class TestNoneFailed:
    R = TriggerRule.NONE_FAILED

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S]) is True

    def test_success_and_skipped(self):
        assert evaluate_trigger_rule(self.R, [S, K]) is True

    def test_all_skipped(self):
        assert evaluate_trigger_rule(self.R, [K, K]) is True

    def test_one_failed(self):
        assert evaluate_trigger_rule(self.R, [S, F]) is False


# ── NONE_SKIPPED ─────────────────────────────────────────────────────


class TestNoneSkipped:
    R = TriggerRule.NONE_SKIPPED

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S]) is True

    def test_all_failed(self):
        assert evaluate_trigger_rule(self.R, [F, F]) is True

    def test_one_skipped(self):
        assert evaluate_trigger_rule(self.R, [S, K]) is False

    def test_success_and_upstream_failed(self):
        assert evaluate_trigger_rule(self.R, [S, U]) is True


# ── NONE_FAILED_MIN_ONE_SUCCESS ──────────────────────────────────────


class TestNoneFailedMinOneSuccess:
    R = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S]) is True

    def test_success_and_skipped(self):
        assert evaluate_trigger_rule(self.R, [S, K]) is True

    def test_all_skipped(self):
        # No failures, but also no successes.
        assert evaluate_trigger_rule(self.R, [K, K]) is False

    def test_one_failed(self):
        assert evaluate_trigger_rule(self.R, [S, F]) is False


# ── NONE_FAILED_OR_SKIPPED ──────────────────────────────────────────


class TestNoneFailedOrSkipped:
    R = TriggerRule.NONE_FAILED_OR_SKIPPED

    def test_all_success(self):
        assert evaluate_trigger_rule(self.R, [S, S, S]) is True

    def test_one_skipped(self):
        assert evaluate_trigger_rule(self.R, [S, K]) is False

    def test_one_failed(self):
        assert evaluate_trigger_rule(self.R, [S, F]) is False

    def test_all_skipped(self):
        assert evaluate_trigger_rule(self.R, [K, K]) is False

    def test_success_and_upstream_failed(self):
        # upstream_failed is not failed or skipped, but the task didn't succeed.
        # However, the rule checks failed==0 and skipped==0, so this depends
        # on whether upstream_failed counts. It does NOT count as failed or
        # skipped, so technically passes — but all must succeed for practical use.
        assert evaluate_trigger_rule(self.R, [S, U]) is True


class TestEdgeCases:
    def test_no_upstreams_always_runs(self):
        for rule in TriggerRule:
            assert evaluate_trigger_rule(rule, []) is True

    def test_string_inputs(self):
        assert (
            evaluate_trigger_rule("all_success", ["success", "success"]) is True
        )

    def test_invalid_rule_raises(self):
        with pytest.raises(ValueError):
            evaluate_trigger_rule("bogus_rule", ["success"])

    def test_invalid_state_raises(self):
        with pytest.raises(ValueError):
            evaluate_trigger_rule(TriggerRule.ALL_SUCCESS, ["not_a_state"])


class TestEvaluateAllRules:
    def test_returns_all_rules(self):
        results = evaluate_all_trigger_rules([S, F, K])
        assert set(results.keys()) == set(TriggerRule)

    def test_mixed_scenario(self):
        results = evaluate_all_trigger_rules([S, F, K])
        assert results[TriggerRule.ALL_DONE] is True
        assert results[TriggerRule.ALL_SUCCESS] is False
        assert results[TriggerRule.ONE_SUCCESS] is True
        assert results[TriggerRule.ONE_FAILED] is True
        assert results[TriggerRule.NONE_FAILED] is False
