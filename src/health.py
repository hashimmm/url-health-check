import json
from dataclasses import dataclass
from time import monotonic
from typing import Literal, Union
import requests
import db
from settings import POSTGRES_URI


@dataclass
class Health:
    status: Literal["ok", "bad", "timeout"]
    code: Union[int, None]
    time_taken: Union[float, None]
    url: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "status": self.status,
                "code": self.code,
                "time_taken": self.time_taken,
                "url": self.url,
            }
        )

    @classmethod
    def from_json(cls, data: str):
        d = json.loads(data)
        return cls(**d)


def check_health(url: str, timeout_secs=0.5) -> Health:
    """Check health for given url."""
    start = monotonic()
    try:
        response = requests.get(url, timeout=timeout_secs)
    except requests.exceptions.Timeout:
        return Health(status="timeout", code=None, time_taken=None, url=url)
    else:
        time_taken = monotonic() - start
        return Health(
            status="ok" if 200 <= response.status_code < 400 else "bad",
            code=response.status_code,
            time_taken=time_taken,
            url=url,
        )


def get_metrics_for_last_5_mins(url: str, db_connection_uri=POSTGRES_URI):
    with db.get_cursor(db_connection_uri) as dbh:
        result = dbh.execute(
            """SELECT
            avg(response_time_secs) as avg_response_time,
            count(*) FILTER (WHERE status_code > 299) as num_bad,
            percentile_disc(0.9) within group (order by response_time_secs) as ninetieth_percentile_response_time
            FROM health_check_data
            where url = %(url)s and recorded_at > now() - interval '5 minutes';
            """,
            {
                "url": url,
            },
        )
        return result.fetchone()
