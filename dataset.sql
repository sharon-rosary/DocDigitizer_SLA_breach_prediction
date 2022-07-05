-- --Query 1
SELECT a.*, b.*, DATEDIFF(MINUTE, FinishedAt, SlaMoment) AS time_diff, e.*
FROM
(
    SELECT Id, DocumentId, SlaMoment, NumberPages, SugestedDocumentClass, RequestedHumanRevision, QualityControl, CustomerName, EntityName
    FROM DocumentMonitor
    WHERE StateText = 'FINISHED'
) AS a
INNER JOIN
(
    SELECT DocumentMonitorId,  MAX(FinishedAt) as FinishedAt
    FROM Job
    GROUP BY DocumentMonitorId
) AS b
ON a.Id = b.DocumentMonitorId
LEFT JOIN
(
    SELECT DocumentMonitorId, SUM(pf_human) AS pf_human, SUM(efv) AS efv, SUM(pfd_topdown) AS pfd_topdown, SUM(pfd_poised) AS pfd_poised, 
    SUM(sfv_human) AS sfv_human, SUM(sfv_ML) AS sfv_ML
    FROM
    (
        SELECT j.Id, j.DocumentMonitorId, JobId, ISNULL(pf_human, 0 ) AS pf_human, ISNULL(efv, 0 ) AS efv, ISNULL(pfd_topdown, 0 ) AS pfd_topdown, 
            ISNULL(pfd_poised, 0 ) AS pfd_poised, ISNULL(sfv_human, 0 ) AS sfv_human, ISNULL(sfv_ML, 0 ) AS sfv_ML
        FROM 
        (
            SELECT Id, DocumentMonitorId FROM Job GROUP BY Id, DocumentMonitorId
        ) AS j
        LEFT JOIN
        (
            SELECT JobId, ISNULL(pf_human, 0 ) AS pf_human, ISNULL(efv, 0 ) AS efv, ISNULL(pfd_topdown, 0 ) AS pfd_topdown, 
            ISNULL(pfd_poised, 0 ) AS pfd_poised, ISNULL(sfv_human, 0 ) AS sfv_human, ISNULL(sfv_ML, 0 ) AS sfv_ML
            FROM
            (
                SELECT JobId, pf_human, efv, pfd_topdown, pfd_poised, sfv_human, sfv_ML
                FROM
                (
                SELECT JobId, CASE SourceMotive 
                    WHEN 'for page filtering - human' THEN 'pf_human'
                    WHEN 'Extract field values' THEN 'efv'
                    WHEN 'page feature detection - top-down' THEN 'pfd_topdown'
                    WHEN 'page feature detection - Poised' THEN 'pfd_poised'
                    WHEN 'To Validate suggested Field Value (From Human)' THEN 'sfv_human'
                    WHEN 'To Validate suggested Field Value (From ML)' THEN 'sfv_ML'
                    ELSE SourceMotive END AS SourceMotive, 1 AS val
                FROM Question
                ) AS t
                PIVOT
                (
                    SUM(val) FOR SourceMotive IN (pf_human, efv, pfd_topdown, pfd_poised, sfv_human, sfv_ML)
                ) AS pt
            ) as ft
        ) AS p
        ON j.Id = p.JobId
    ) as d
    GROUP BY DocumentMonitorId
) AS e
ON b.DocumentMonitorId = e.DocumentMonitorId;


-- Query 2
SELECT DocumentMonitorId, SUM(pf_human) AS pf_human, SUM(efv) AS efv, SUM(pfd_topdown) AS pfd_topdown, SUM(pfd_poised) AS pfd_poised, 
SUM(sfv_human) AS sfv_human, SUM(sfv_ML) AS sfv_ML
FROM
(
    SELECT j.Id, j.DocumentMonitorId, JobId, ISNULL(pf_human, 0 ) AS pf_human, ISNULL(efv, 0 ) AS efv, ISNULL(pfd_topdown, 0 ) AS pfd_topdown, 
        ISNULL(pfd_poised, 0 ) AS pfd_poised, ISNULL(sfv_human, 0 ) AS sfv_human, ISNULL(sfv_ML, 0 ) AS sfv_ML
    FROM 
    (
        SELECT Id, DocumentMonitorId FROM Job GROUP BY Id, DocumentMonitorId
    ) AS j
    LEFT JOIN
    (
        SELECT JobId, ISNULL(pf_human, 0 ) AS pf_human, ISNULL(efv, 0 ) AS efv, ISNULL(pfd_topdown, 0 ) AS pfd_topdown, 
        ISNULL(pfd_poised, 0 ) AS pfd_poised, ISNULL(sfv_human, 0 ) AS sfv_human, ISNULL(sfv_ML, 0 ) AS sfv_ML
        FROM
        (
            SELECT JobId, pf_human, efv, pfd_topdown, pfd_poised, sfv_human, sfv_ML
            FROM
            (
            SELECT JobId, CASE SourceMotive 
                WHEN 'for page filtering - human' THEN 'pf_human'
                WHEN 'Extract field values' THEN 'efv'
                WHEN 'page feature detection - top-down' THEN 'pfd_topdown'
                WHEN 'page feature detection - Poised' THEN 'pfd_poised'
                WHEN 'To Validate suggested Field Value (From Human)' THEN 'sfv_human'
                WHEN 'To Validate suggested Field Value (From ML)' THEN 'sfv_ML'
                ELSE SourceMotive END AS SourceMotive, 1 AS val
            FROM Question
            ) AS t
            PIVOT
            (
                SUM(val) FOR SourceMotive IN (pf_human, efv, pfd_topdown, pfd_poised, sfv_human, sfv_ML)
            ) AS pt
        ) as ft
    ) AS p
    ON j.Id = p.JobId
) as d
GROUP BY DocumentMonitorId;