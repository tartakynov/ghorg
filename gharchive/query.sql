/*
https://bigquery.cloud.google.com/table/githubarchive:year.2014

This query returns GitHub organizations which repositories were
forked more than 10 times in 2014
*/

SELECT
  repository_organization,
  SUM(forks) AS forks
FROM (
  SELECT
    LOWER(repository_organization) AS repository_organization,
    COUNT(*) AS forks
  FROM
    [githubarchive:year.2014]
  WHERE
    type = 'ForkEvent'
    AND repository_organization IS NOT NULL
  GROUP BY
    repository_organization )
GROUP BY
  repository_organization
HAVING
  SUM(forks) > 10
ORDER BY
  2 DESC
