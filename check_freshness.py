"""Alert when the scraper has silently stopped producing jobs.

The nightly run exits 0 when nobody is eligible ("No active users ..."), so a
dead timer, a missing unit or a swallowed failure all look like a healthy
no-op. This check runs on its own timer and compares two facts: are there
eligible users (resume + login in the last 30 days), and how old is the newest
jobs.date_pulled. Eligible users plus a stale newest job means the scraper is
not doing its job.

Read-only. Exit 0 = healthy or nothing to do, exit 1 = stale (job_wrapper.sh
turns any exit other than 0 or 2 into a Slack alert).
"""
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

MAX_AGE_DAYS = 2


@dataclass
class Freshness:
    stale: bool
    message: str


def evaluate(eligible_users: int, newest_pulled: Optional[datetime], now: datetime,
             max_age_days: int = MAX_AGE_DAYS) -> Freshness:
    if eligible_users == 0:
        return Freshness(False, "OK: no eligible users (resume + login in last 30 days), nothing expected.")

    if newest_pulled is None:
        return Freshness(True, f"STALE: {eligible_users} eligible user(s) but the jobs table has no date_pulled at all.")

    age = now - newest_pulled
    if age > timedelta(days=max_age_days):
        return Freshness(True, f"STALE: {eligible_users} eligible user(s) but the newest job was pulled "
                               f"{age.days} days ago ({newest_pulled.date().isoformat()}). "
                               f"Check job-scout.timer and ~/logs/job-scout.log on haguerale-server.")

    return Freshness(False, f"OK: {eligible_users} eligible user(s), newest job pulled {newest_pulled.date().isoformat()}.")


def get_newest_date_pulled() -> Optional[datetime]:
    from persistent_storage import get_supabase_client

    response = (get_supabase_client().table('jobs')
                .select('date_pulled')
                .order('date_pulled', desc=True)
                .limit(1)
                .execute())
    if not response.data or not response.data[0].get('date_pulled'):
        return None
    pulled = datetime.fromisoformat(response.data[0]['date_pulled'])
    # date_pulled is written from datetime.now() (naive, server local time)
    return pulled if pulled.tzinfo else pulled.astimezone()


def main() -> int:
    from persistent_storage import get_active_users_with_resume

    result = evaluate(len(get_active_users_with_resume()), get_newest_date_pulled(), datetime.now(timezone.utc))
    print(result.message)
    return 1 if result.stale else 0


if __name__ == '__main__':
    sys.exit(main())
