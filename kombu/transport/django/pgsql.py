APP_ID = 100001

LOCK_JOB = """
WITH RECURSIVE job AS (
  SELECT (j).*, pg_try_advisory_lock(%(app_id)s, (j).id) AS locked
  FROM (
    SELECT j
    FROM djkombu_message AS j
    JOIN djkombu_queue AS q ON j.queue_id = q.id
    WHERE q.name = %(queue)s
    ORDER BY j.id
    LIMIT 1
  ) AS t1
  UNION ALL (
    SELECT (j).*, pg_try_advisory_lock(%(app_id)s, (j).id) AS locked
    FROM (
      SELECT (
        SELECT j
        FROM djkombu_message AS j
        JOIN djkombu_queue AS q ON j.queue_id = q.id
        WHERE q.name = %(queue)s
        AND j.id > job.id
        ORDER BY j.id
        LIMIT 1
      ) AS j
    FROM job
    WHERE NOT job.locked
    LIMIT 1
    ) AS t1
  )
)
SELECT id, queue_id, payload, sent_at
FROM job
WHERE locked
LIMIT 1
"""

UNLOCK = """
SELECT pg_advisory_unlock(%(app_id)s, %(lock_id)s)
"""