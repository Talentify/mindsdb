from datetime import datetime, timezone

from mindsdb.integrations.handlers.github_handler.github_tables import GithubPullRequestsTable
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


class _User:
    login = "patrick"


class _Ref:
    def __init__(self, ref: str):
        self.ref = ref


class _Pull:
    def __init__(self, number: int):
        self.number = number
        self.title = f"PR {number}"
        self.state = "closed"
        self.user = _User()
        self.labels = []
        self.milestone = None
        self.assignees = []
        self.requested_reviewers = []
        self.requested_teams = []
        self.draft = False
        self.body = ""
        self.base = _Ref("main")
        self.head = _Ref(f"branch-{number}")
        self.created_at = datetime(2026, 5, 1, tzinfo=timezone.utc)
        self.updated_at = datetime(2026, 5, 2, tzinfo=timezone.utc)
        self.merged_at = datetime(2026, 5, 29, tzinfo=timezone.utc)
        self.closed_at = self.merged_at


class _Repo:
    full_name = "Talentify/mktplace-front"

    def __init__(self, pull_count: int):
        self.pulls = [_Pull(number) for number in range(1, pull_count + 1)]

    def get_pulls(self, **_kwargs):
        return iter(self.pulls)


class _Handler:
    def __init__(self, repo: _Repo):
        self.repo = repo

    def get_repos(self, conditions):
        for condition in conditions:
            if condition.column == "repository" and condition.op == FilterOperator.EQUAL:
                condition.applied = True
        return [self.repo]


def test_pull_requests_scan_beyond_limit_when_query_has_unapplied_filters():
    table = GithubPullRequestsTable(_Handler(_Repo(pull_count=35)))
    conditions = [
        FilterCondition("repository", FilterOperator.EQUAL, "Talentify/mktplace-front"),
        FilterCondition("state", FilterOperator.EQUAL, "closed"),
        FilterCondition("merged", FilterOperator.GREATER_THAN_OR_EQUAL, "2026-05-25"),
    ]

    df = table.list(conditions=conditions, limit=20, targets=["number", "merged", "repository"])

    assert len(df) == 35
    assert df["number"].max() == 35


def test_pull_requests_keep_requested_limit_when_all_filters_are_native():
    table = GithubPullRequestsTable(_Handler(_Repo(pull_count=35)))
    conditions = [
        FilterCondition("repository", FilterOperator.EQUAL, "Talentify/mktplace-front"),
        FilterCondition("state", FilterOperator.EQUAL, "closed"),
        FilterCondition("base", FilterOperator.EQUAL, "main"),
    ]

    df = table.list(conditions=conditions, limit=20, targets=["number", "base", "repository"])

    assert len(df) == 20
    assert df["number"].max() == 20
