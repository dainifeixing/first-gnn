from __future__ import annotations

import unittest


class PackageApiTests(unittest.TestCase):
    def test_refactored_symbols_are_exposed_at_package_level(self) -> None:
        from txflow import (
            GraphDatasetSummary,
            LedgerReviewRow,
            MirrorAnnotationRow,
            MirrorReviewRow,
            NormalizedTransaction,
            OperatingThresholdRecommendation,
            OwnerAnnotation,
            OwnerLookup,
            OwnerReviewRow,
            OwnerSummaryReport,
            OwnerSummaryRow,
            RuleAuditRow,
            RoundBootstrap,
            RoundReport,
            RoleAnnotation,
            RoleLookup,
            ThresholdSweepReport,
            build_ledger_review_rows,
            build_rule_audit_rows,
            build_owner_review_roles,
            build_owner_review_rows,
            build_round_report,
            build_mirror_annotations,
            build_mirror_review_rows,
            export_ledger_review_csv,
            export_normalized_ledgers,
            export_mirror_annotations_csv,
            export_mirror_review_csv,
            export_owner_review_csv,
            export_owner_summary_json,
            export_rule_audit_csv,
            export_role_annotations_csv,
            load_mirror_annotations,
            load_normalized_ledgers,
            load_owner_annotations,
            load_role_annotations,
            review_workload_forecast,
            score_threshold_sweep,
            summarize_graph_dataset,
            summarize_owner_activity,
        )

        self.assertTrue(callable(build_round_report))
        self.assertTrue(callable(build_mirror_review_rows))
        self.assertTrue(callable(build_mirror_annotations))
        self.assertTrue(callable(build_ledger_review_rows))
        self.assertTrue(callable(build_rule_audit_rows))
        self.assertTrue(callable(export_normalized_ledgers))
        self.assertTrue(callable(export_ledger_review_csv))
        self.assertTrue(callable(export_rule_audit_csv))
        self.assertTrue(callable(export_mirror_review_csv))
        self.assertTrue(callable(export_mirror_annotations_csv))
        self.assertTrue(callable(summarize_graph_dataset))
        self.assertTrue(callable(summarize_owner_activity))
        self.assertTrue(callable(build_owner_review_rows))
        self.assertTrue(callable(build_owner_review_roles))
        self.assertTrue(callable(export_owner_review_csv))
        self.assertTrue(callable(export_owner_summary_json))
        self.assertTrue(callable(export_role_annotations_csv))
        self.assertTrue(callable(score_threshold_sweep))
        self.assertTrue(callable(review_workload_forecast))
        self.assertTrue(callable(load_normalized_ledgers))
        self.assertTrue(callable(load_mirror_annotations))
        self.assertTrue(callable(load_owner_annotations))
        self.assertTrue(callable(load_role_annotations))
        self.assertTrue(hasattr(RoundReport, "__dataclass_fields__"))
        self.assertTrue(hasattr(RoundBootstrap, "__dataclass_fields__"))
        self.assertTrue(hasattr(OwnerAnnotation, "__dataclass_fields__"))
        self.assertTrue(hasattr(OwnerLookup, "resolve"))
        self.assertTrue(hasattr(RoleAnnotation, "__dataclass_fields__"))
        self.assertTrue(hasattr(RoleLookup, "resolve"))
        self.assertTrue(hasattr(NormalizedTransaction, "__dataclass_fields__"))
        self.assertTrue(hasattr(GraphDatasetSummary, "__dataclass_fields__"))
        self.assertTrue(hasattr(LedgerReviewRow, "__dataclass_fields__"))
        self.assertTrue(hasattr(RuleAuditRow, "__dataclass_fields__"))
        self.assertTrue(hasattr(MirrorReviewRow, "__dataclass_fields__"))
        self.assertTrue(hasattr(MirrorAnnotationRow, "__dataclass_fields__"))
        self.assertTrue(hasattr(OwnerSummaryRow, "__dataclass_fields__"))
        self.assertTrue(hasattr(OwnerSummaryReport, "__dataclass_fields__"))
        self.assertTrue(hasattr(OwnerReviewRow, "__dataclass_fields__"))
        self.assertTrue(hasattr(ThresholdSweepReport, "__dataclass_fields__"))
        self.assertTrue(hasattr(OperatingThresholdRecommendation, "__dataclass_fields__"))


if __name__ == "__main__":
    unittest.main()
