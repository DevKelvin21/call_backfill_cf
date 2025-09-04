-- Standard SQL

MERGE `dev-at-cf.at_dials.at_dials_cleaned` AS T
USING (
  -- Source rows from staging, filtered to only those NOT present in the existing table
  SELECT
    S.Date,
    S.FirstName,
    S.LastName,
    S.CallNotes,
    S.Phone,
    S.Email,
    S.LeadID,
    S.SiteName,
    S.ListID,
    S.Disposition,
    S.TalkTime,
    S.TermReason,
    S.SubscriberID,
    S.ListDescription,
    S.Source,
    S.LeadType,
    S.Address,
    S.SourceLocalTime,
    S.SourceTimezone,
    S.SourceFile,
    S.RowHash,
    S.DedupKey,
    S.IngestedAt,

    -- Casted helpers for joins vs TARGET
    CAST(S.Phone       AS STRING) AS PhoneS,
    CAST(S.LeadID      AS STRING) AS LeadIDS,
    CAST(S.ListID      AS STRING) AS ListIDS,
    CAST(S.Disposition AS STRING) AS DispositionS,
    CAST(S.FirstName   AS STRING) AS FirstNameS,
    CAST(S.LastName    AS STRING) AS LastNameS
  FROM `dev-at-cf.at_dials_stage.at_dials_stage` AS S

  -- Anti-join against the existing table to remove already-present calls
  LEFT JOIN (
    SELECT
      CAST(E.Phone       AS STRING) AS PhoneS,
      CAST(E.LeadID      AS STRING) AS LeadIDS,
      CAST(E.ListID      AS STRING) AS ListIDS,
      CAST(E.Disposition AS STRING) AS DispositionS,
      CAST(E.FirstName   AS STRING) AS FirstNameS,
      CAST(E.LastName    AS STRING) AS LastNameS,
      E.Date
    FROM `dev-at-cf.at_dials.at_dials_vici_cf_bq` AS E
  ) AS E
  ON COALESCE(CAST(S.Phone AS STRING), '') = COALESCE(E.PhoneS, '')
     AND (
       (
         COALESCE(CAST(S.LeadID AS STRING), '')      = COALESCE(E.LeadIDS, '')
         AND COALESCE(CAST(S.ListID AS STRING), '')  = COALESCE(E.ListIDS, '')
         AND COALESCE(CAST(S.Disposition AS STRING), '') = COALESCE(E.DispositionS, '')
         AND ABS(TIMESTAMP_DIFF(S.Date, E.Date, SECOND)) <= @time_tol
       )
       OR
       (
         S.LeadID IS NULL AND E.LeadIDS IS NULL
         AND COALESCE(CAST(S.FirstName AS STRING), '') = COALESCE(E.FirstNameS, '')
         AND COALESCE(CAST(S.LastName  AS STRING), '') = COALESCE(E.LastNameS, '')
         AND COALESCE(CAST(S.ListID    AS STRING), '') = COALESCE(E.ListIDS, '')
         AND COALESCE(CAST(S.Disposition AS STRING), '') = COALESCE(E.DispositionS, '')
         AND ABS(TIMESTAMP_DIFF(S.Date, E.Date, SECOND)) <= @time_tol
       )
     )

  WHERE E.PhoneS IS NULL  -- keep only rows NOT matched in the existing table
) AS SRC
ON (
  -- De-dupe vs TARGET (cleaned table) with tolerance, using casts on T
  (
    COALESCE(SRC.PhoneS,'')         = COALESCE(CAST(T.Phone       AS STRING),'')
    AND COALESCE(SRC.LeadIDS,'')    = COALESCE(CAST(T.LeadID      AS STRING),'')
    AND COALESCE(SRC.ListIDS,'')    = COALESCE(CAST(T.ListID      AS STRING),'')
    AND COALESCE(SRC.DispositionS,'') = COALESCE(CAST(T.Disposition AS STRING),'')
    AND ABS(TIMESTAMP_DIFF(SRC.Date, T.Date, SECOND)) <= @time_tol
  )
  OR
  (
    SRC.LeadIDS IS NULL AND T.LeadID IS NULL
    AND COALESCE(SRC.PhoneS,'')      = COALESCE(CAST(T.Phone     AS STRING),'')
    AND COALESCE(SRC.FirstNameS,'')  = COALESCE(CAST(T.FirstName AS STRING),'')
    AND COALESCE(SRC.LastNameS,'')   = COALESCE(CAST(T.LastName  AS STRING),'')
    AND COALESCE(SRC.ListIDS,'')     = COALESCE(CAST(T.ListID    AS STRING),'')
    AND COALESCE(SRC.DispositionS,'')= COALESCE(CAST(T.Disposition AS STRING),'')
    AND ABS(TIMESTAMP_DIFF(SRC.Date, T.Date, SECOND)) <= @time_tol
  )
)
WHEN NOT MATCHED THEN
  INSERT (
    Date, FirstName, LastName, CallNotes, Phone, Email, LeadID, SiteName, ListID,
    Disposition, TalkTime, TermReason, SubscriberID, ListDescription, Source, LeadType,
    Address, SourceLocalTime, SourceTimezone, SourceFile, RowHash, DedupKey, IngestedAt
  )
  VALUES (
    SRC.Date, SRC.FirstName, SRC.LastName, SRC.CallNotes, SRC.Phone, SRC.Email, SRC.LeadID, SRC.SiteName, SRC.ListID,
    SRC.Disposition, SRC.TalkTime, SRC.TermReason, SRC.SubscriberID, SRC.ListDescription, SRC.Source, SRC.LeadType,
    SRC.Address, SRC.SourceLocalTime, SRC.SourceTimezone, SRC.SourceFile, SRC.RowHash, SRC.DedupKey, SRC.IngestedAt
  );
