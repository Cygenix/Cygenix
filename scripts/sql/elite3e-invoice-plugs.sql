/* =============================================================================
   Elite 3E - plug TimeCard / TimeBill / CostCard / CostBill under invoices
   that have no detail beneath them
   =============================================================================

   WHY THIS EXISTS
   ---------------
   After a conversion (or a partial purge) an InvMaster row can be left with a fee
   and/or hard-cost total but no TimeCard / CostCard rows pointing at it. 3E's
   invoice-level screens still work, but anything that walks the detail (bill
   history, realisation, collections by timekeeper, reversal, reprint) either
   shows nothing or refuses. The detection query the fix is built around is the
   one the business uses to size the problem:

       SELECT SUM(im.OrgFee)
       FROM   dbo.InvMaster im
       WHERE  NOT EXISTS (SELECT 1 FROM dbo.TimeCard tc WHERE tc.InvMaster = im.InvIndex)
         AND  im.IsReversed = 0
         AND  im.OrgFee > 0

   This script creates ONE plug card and ONE plug bill line per invoice per side:

       fee side   OrgFee  > 0 and no TimeCard  ->  TimeCard + TimeBill   (FEES)
       cost side  OrgHCo  > 0 and no CostCard  ->  CostCard + CostBill   (HCOST)

   The cost side mirrors the fee logic the business gave; it is switchable off
   below if only fees are wanted (@DoCosts).

   HOW TO SPOT A PLUG LATER
   ------------------------
   Every plug row carries the marker in every load/conversion/narrative field the
   table has:  LoadNumber, LoadSource, LoadGroup, ConvSource, ConvGroup,
   Narrative, Narrative_UnformattedText.  [TimeStamp] is today's date, and
   InternalComments says which invoice it plugs and when. ConversionRefNum holds
   the InvNumber so a plug can be traced without a join.

   SAFETY
   ------
   * Runs in PREVIEW by default (@Commit = 0): it does everything inside a
     transaction, prints what it would insert, then rolls back. Set @Commit = 1
     to keep the rows. Run it in preview first and read the SKIPPED list.
   * Idempotent: once a plug exists the NOT EXISTS test fails, so a re-run
     inserts nothing for that invoice.
   * Index columns: 3E installs differ on whether TimeIndex / CostIndex /
     TimeBillIndex / CostBillIndex are IDENTITY. The script assigns explicit
     MAX+n values either way and turns IDENTITY_INSERT on only where the column
     is an identity, so it does not have to know in advance.
   * Nothing is deleted. A commented "undo" block is at the bottom.

   ASSUMPTIONS ("educated guesses") - check each against your install
   -------------------------------------------------------------------
   1. Effective-dated keys. WorkMattEffDate / BillMattEffDate come from
      dbo.MattDate (MatterLkp, NxStartDate, NxEndDate) and TkprEffDate from
      dbo.TkprDate (TimekeeperLkp, NxStartDate, NxEndDate), choosing the row in
      force on the invoice date and falling back to the latest row.
   2. ARMaster. Every posted invoice has at least one dbo.ARMaster row with
      ARMaster.InvMaster = InvIndex. A multi-payor invoice has several; the
      lowest ARIndex is used. An invoice with none is SKIPPED and listed.
   3. Office / Timekeeper. Office = InvMaster.BillingOffice, else Matter.Office,
      else @DefaultOffice. Timekeeper = BillTimekeeper, else RespTimekeeper,
      else SendTimekeeper, else @DefaultTimekeeper. Missing -> SKIPPED.
   4. Collected / written-off / balance on the bill lines are derived from the
      invoice totals under the 3E convention Bal = Org + Adj - Paid, with a
      negative Adj treated as a write-off:
          WOff = -Adj (when Adj < 0)      Bal = BalFee      Coll = Org + Adj - Bal
      so Coll + WOff + Bal = Org. If your AR convention is different, change
      the three expressions in the #Inv build (they are marked).
   5. Hours. A fee plug carries @PlugHours (default 0.00) so it does not invent
      billable hours; WorkRate is then 0 and WorkAmt carries the fee. Set
      @PlugHours = 1 if your reports insist on Hrs x Rate = Amt.
   6. Cost quantity is 1 with rate = amount. CostType defaults to '100' by
      analogy with TimeType = '100' - confirm it exists in your CostType list.
   7. ArchetypeCode is copied from the most common value already in each table
      (falls back to the table name), so it matches whatever the install uses.
   8. No ProfMaster (proforma) is created. ProfMaster stays NULL on the plug.
   9. Language column is int in these tables, so 1033 is declared as int.

   ============================================================================= */

SET NOCOUNT ON;
SET XACT_ABORT ON;

/* ---------------------------------------------------------------------------
   Switches and defaults
   --------------------------------------------------------------------------- */
DECLARE @Commit           BIT           = 0;        -- 0 = preview + rollback, 1 = write
DECLARE @DoFees           BIT           = 1;        -- create TimeCard + TimeBill plugs
DECLARE @DoCosts          BIT           = 1;        -- create CostCard + CostBill plugs
DECLARE @IncludeSoftCost  BIT           = 0;        -- 1 = OrgSCo is added into the HCOST plug amount

-- TIME defaults (as supplied)
DECLARE @Language         INT           = 1033;
DECLARE @TimeType         NVARCHAR(32)  = N'100';
DECLARE @WorkType         NVARCHAR(32)  = N'100';
DECLARE @TimeTranType     NVARCHAR(32)  = N'FEES';
DECLARE @RateCalcList     NVARCHAR(16)  = N'ActRate';
DECLARE @LoadNumber       NVARCHAR(128) = N'Plug';

-- COST defaults ("as above" but HCOST)
DECLARE @CostType         NVARCHAR(32)  = N'100';   -- assumption 6
DECLARE @CostTranType     NVARCHAR(32)  = N'HCOST';

-- Marker written into every load / conv / narrative field
DECLARE @Plug             NVARCHAR(128) = N'Plug';

-- Today's date (no time part) for [TimeStamp]
DECLARE @Today            DATETIME      = CAST(CAST(GETDATE() AS DATE) AS DATETIME);

-- Shape of the fee plug
DECLARE @PlugHours        DECIMAL(16,2) = 0.00;     -- assumption 5
DECLARE @PlugQty          DECIMAL(16,2) = 1.00;     -- cost quantity

-- Last-resort fallbacks when the invoice and matter give nothing
DECLARE @DefaultOffice     NVARCHAR(32) = NULL;     -- e.g. N'LON'
DECLARE @DefaultTimekeeper INT          = NULL;     -- e.g. 1

/* ---------------------------------------------------------------------------
   Before picture - the business's own detection query, fee and cost side
   --------------------------------------------------------------------------- */
SELECT  Stage        = 'BEFORE',
        FeeInvoices  = SUM(x.NeedFee),
        FeeToPlug    = SUM(CASE WHEN x.NeedFee  = 1 THEN im.OrgFee ELSE 0 END),
        CostInvoices = SUM(x.NeedCost),
        CostToPlug   = SUM(CASE WHEN x.NeedCost = 1 THEN c.CostOrg ELSE 0 END)
FROM    dbo.InvMaster im
CROSS APPLY (SELECT CostOrg = ISNULL(im.OrgHCo,0) + CASE WHEN @IncludeSoftCost = 1 THEN ISNULL(im.OrgSCo,0) ELSE 0 END) c
CROSS APPLY (SELECT NeedFee  = CASE WHEN im.OrgFee > 0
                                     AND NOT EXISTS (SELECT 1 FROM dbo.TimeCard tc WHERE tc.InvMaster = im.InvIndex)
                                    THEN 1 ELSE 0 END,
                    NeedCost = CASE WHEN c.CostOrg > 0
                                     AND NOT EXISTS (SELECT 1 FROM dbo.CostCard cc WHERE cc.InvMaster = im.InvIndex)
                                    THEN 1 ELSE 0 END) x
WHERE   im.IsReversed = 0;

/* ---------------------------------------------------------------------------
   Archetypes - copy what the install already uses
   --------------------------------------------------------------------------- */
DECLARE @ArchTimeCard NVARCHAR(100), @ArchTimeBill NVARCHAR(100),
        @ArchCostCard NVARCHAR(100), @ArchCostBill NVARCHAR(100);

SELECT TOP (1) @ArchTimeCard = ArchetypeCode FROM dbo.TimeCard WHERE ArchetypeCode <> N'' GROUP BY ArchetypeCode ORDER BY COUNT_BIG(*) DESC;
SELECT TOP (1) @ArchTimeBill = ArchetypeCode FROM dbo.TimeBill WHERE ArchetypeCode <> N'' GROUP BY ArchetypeCode ORDER BY COUNT_BIG(*) DESC;
SELECT TOP (1) @ArchCostCard = ArchetypeCode FROM dbo.CostCard WHERE ArchetypeCode <> N'' GROUP BY ArchetypeCode ORDER BY COUNT_BIG(*) DESC;
SELECT TOP (1) @ArchCostBill = ArchetypeCode FROM dbo.CostBill WHERE ArchetypeCode <> N'' GROUP BY ArchetypeCode ORDER BY COUNT_BIG(*) DESC;

SET @ArchTimeCard = ISNULL(@ArchTimeCard, N'TimeCard');
SET @ArchTimeBill = ISNULL(@ArchTimeBill, N'TimeBill');
SET @ArchCostCard = ISNULL(@ArchCostCard, N'CostCard');
SET @ArchCostBill = ISNULL(@ArchCostBill, N'CostBill');

/* ---------------------------------------------------------------------------
   Build the candidate list: one row per invoice with everything both sides
   need, derived from InvMaster (and Matter / ARMaster / MattDate / TkprDate)
   --------------------------------------------------------------------------- */
IF OBJECT_ID('tempdb..#Inv') IS NOT NULL DROP TABLE #Inv;

SELECT
    im.InvIndex,
    im.InvNumber,
    im.Currency,

    -- dates: prefer the invoice's own, fall back along the chain
    d.WorkDate,
    PostDate        = COALESCE(im.PostDate, im.InvDate, im.TranDate, im.GLDate),
    GLDate          = COALESCE(im.GLDate,   im.PostDate, im.InvDate, im.TranDate),
    CurrDate        = COALESCE(im.CurDate,  im.InvDate,  im.PostDate, im.TranDate),

    -- who / where
    Office          = COALESCE(im.BillingOffice, m.Office, @DefaultOffice),
    Matter          = im.LeadMatter,
    Timekeeper      = COALESCE(im.BillTimekeeper, im.RespTimekeeper, im.SendTimekeeper, @DefaultTimekeeper),
    ARIndex         = ar.ARIndex,
    MattEffDate     = md.MattDateID,
    TkprEffDate     = td.TkprDateID,

    -- exchange rates carried across as-is
    UnitCurrRate    = ISNULL(im.UnitCurrRate, 1),
    FirmCurrRate    = ISNULL(im.FirmCurrRate, 1),
    im.Rpt1CurrRate, im.Rpt2CurrRate, im.Rpt3CurrRate,

    -- fee side
    NeedFee         = CASE WHEN @DoFees = 1 AND im.OrgFee > 0
                            AND NOT EXISTS (SELECT 1 FROM dbo.TimeCard tc WHERE tc.InvMaster = im.InvIndex)
                           THEN 1 ELSE 0 END,
    FeeOrg          = im.OrgFee,
    FeeWOff         = CASE WHEN ISNULL(im.AdjFee,0) < 0 THEN -im.AdjFee ELSE 0 END,        -- assumption 4
    FeeBal          = ISNULL(im.BalFee, 0),                                                  -- assumption 4
    FeeColl         = im.OrgFee + ISNULL(im.AdjFee,0) - ISNULL(im.BalFee,0),                -- assumption 4

    -- cost side (hard cost, optionally + soft cost)
    NeedCost        = CASE WHEN @DoCosts = 1 AND c.CostOrg > 0
                            AND NOT EXISTS (SELECT 1 FROM dbo.CostCard cc WHERE cc.InvMaster = im.InvIndex)
                           THEN 1 ELSE 0 END,
    c.CostOrg,
    CostWOff        = CASE WHEN c.CostAdj < 0 THEN -c.CostAdj ELSE 0 END,                  -- assumption 4
    CostBal         = c.CostBal,                                                             -- assumption 4
    CostColl        = c.CostOrg + c.CostAdj - c.CostBal,                                     -- assumption 4

    -- assigned below
    TimeIndex       = CAST(NULL AS INT),
    TimeBillIndex   = CAST(NULL AS INT),
    CostIndex       = CAST(NULL AS INT),
    CostBillIndex   = CAST(NULL AS INT),
    SkipReason      = CAST(NULL AS NVARCHAR(200))
INTO #Inv
FROM dbo.InvMaster im
CROSS APPLY (SELECT WorkDate = COALESCE(im.InvDate, im.TranDate, im.PostDate, im.GLDate)) d
CROSS APPLY (SELECT CostOrg = ISNULL(im.OrgHCo,0) + CASE WHEN @IncludeSoftCost = 1 THEN ISNULL(im.OrgSCo,0) ELSE 0 END,
                    CostAdj = ISNULL(im.AdjHCo,0) + CASE WHEN @IncludeSoftCost = 1 THEN ISNULL(im.AdjSCo,0) ELSE 0 END,
                    CostBal = ISNULL(im.BalHCo,0) + CASE WHEN @IncludeSoftCost = 1 THEN ISNULL(im.BalSCo,0) ELSE 0 END) c
LEFT JOIN dbo.Matter m ON m.MattIndex = im.LeadMatter
OUTER APPLY (SELECT TOP (1) a.ARIndex
             FROM   dbo.ARMaster a
             WHERE  a.InvMaster = im.InvIndex
             ORDER BY a.ARIndex) ar                                                          -- assumption 2
OUTER APPLY (SELECT TOP (1) x.MattDateID
             FROM   dbo.MattDate x
             WHERE  x.MatterLkp = im.LeadMatter
             ORDER BY CASE WHEN x.NxStartDate <= d.WorkDate AND (x.NxEndDate IS NULL OR x.NxEndDate >= d.WorkDate) THEN 0 ELSE 1 END,
                      x.NxStartDate DESC) md                                                 -- assumption 1
OUTER APPLY (SELECT TOP (1) x.TkprDateID
             FROM   dbo.TkprDate x
             WHERE  x.TimekeeperLkp = COALESCE(im.BillTimekeeper, im.RespTimekeeper, im.SendTimekeeper, @DefaultTimekeeper)
             ORDER BY CASE WHEN x.NxStartDate <= d.WorkDate AND (x.NxEndDate IS NULL OR x.NxEndDate >= d.WorkDate) THEN 0 ELSE 1 END,
                      x.NxStartDate DESC) td                                                 -- assumption 1
WHERE im.IsReversed = 0
  AND (   (@DoFees  = 1 AND im.OrgFee  > 0 AND NOT EXISTS (SELECT 1 FROM dbo.TimeCard tc WHERE tc.InvMaster = im.InvIndex))
       OR (@DoCosts = 1 AND c.CostOrg  > 0 AND NOT EXISTS (SELECT 1 FROM dbo.CostCard cc WHERE cc.InvMaster = im.InvIndex)) );

/* Anything that cannot satisfy a NOT NULL column is skipped, not guessed */
UPDATE #Inv SET SkipReason =
    STUFF(  CASE WHEN Matter      IS NULL THEN N', no LeadMatter'                 ELSE N'' END
          + CASE WHEN Office      IS NULL THEN N', no Office'                     ELSE N'' END
          + CASE WHEN Timekeeper  IS NULL THEN N', no Timekeeper'                 ELSE N'' END
          + CASE WHEN ARIndex     IS NULL THEN N', no ARMaster'                   ELSE N'' END
          + CASE WHEN MattEffDate IS NULL THEN N', no MattDate row'               ELSE N'' END
          + CASE WHEN TkprEffDate IS NULL THEN N', no TkprDate row'               ELSE N'' END
          + CASE WHEN WorkDate    IS NULL THEN N', no usable date on invoice'     ELSE N'' END
          + CASE WHEN Currency    IS NULL THEN N', no Currency'                   ELSE N'' END
          , 1, 2, N'')
WHERE Matter IS NULL OR Office IS NULL OR Timekeeper IS NULL OR ARIndex IS NULL
   OR MattEffDate IS NULL OR TkprEffDate IS NULL OR WorkDate IS NULL OR Currency IS NULL;

/* ---------------------------------------------------------------------------
   Assign the new index values: MAX + running number, in InvIndex order
   --------------------------------------------------------------------------- */
DECLARE @MaxTime INT, @MaxTimeBill INT, @MaxCost INT, @MaxCostBill INT;
SELECT @MaxTime     = ISNULL(MAX(TimeIndex),     0) FROM dbo.TimeCard;
SELECT @MaxTimeBill = ISNULL(MAX(TimeBillIndex), 0) FROM dbo.TimeBill;
SELECT @MaxCost     = ISNULL(MAX(CostIndex),     0) FROM dbo.CostCard;
SELECT @MaxCostBill = ISNULL(MAX(CostBillIndex), 0) FROM dbo.CostBill;

;WITH f AS (SELECT TimeIndex, TimeBillIndex, rn = ROW_NUMBER() OVER (ORDER BY InvIndex)
            FROM #Inv WHERE NeedFee = 1 AND SkipReason IS NULL)
UPDATE f SET TimeIndex = @MaxTime + rn, TimeBillIndex = @MaxTimeBill + rn;

;WITH c AS (SELECT CostIndex, CostBillIndex, rn = ROW_NUMBER() OVER (ORDER BY InvIndex)
            FROM #Inv WHERE NeedCost = 1 AND SkipReason IS NULL)
UPDATE c SET CostIndex = @MaxCost + rn, CostBillIndex = @MaxCostBill + rn;

/* ---------------------------------------------------------------------------
   Preview: what will be written, and what is being skipped
   --------------------------------------------------------------------------- */
SELECT  Stage = 'PLAN',
        FeePlugs   = SUM(CASE WHEN NeedFee  = 1 AND SkipReason IS NULL THEN 1 ELSE 0 END),
        FeeAmount  = SUM(CASE WHEN NeedFee  = 1 AND SkipReason IS NULL THEN FeeOrg  ELSE 0 END),
        CostPlugs  = SUM(CASE WHEN NeedCost = 1 AND SkipReason IS NULL THEN 1 ELSE 0 END),
        CostAmount = SUM(CASE WHEN NeedCost = 1 AND SkipReason IS NULL THEN CostOrg ELSE 0 END),
        Skipped    = SUM(CASE WHEN SkipReason IS NOT NULL THEN 1 ELSE 0 END),
        Mode       = CASE WHEN @Commit = 1 THEN 'COMMIT' ELSE 'PREVIEW (rolled back)' END
FROM #Inv;

SELECT  Stage = 'SKIPPED', InvIndex, InvNumber, FeeOrg, CostOrg, SkipReason
FROM    #Inv
WHERE   SkipReason IS NOT NULL
ORDER BY InvIndex;

SELECT  Stage = 'ROWS', InvIndex, InvNumber, Currency, WorkDate, Office, Matter, Timekeeper, ARIndex,
        NeedFee, FeeOrg, FeeColl, FeeWOff, FeeBal, TimeIndex, TimeBillIndex,
        NeedCost, CostOrg, CostColl, CostWOff, CostBal, CostIndex, CostBillIndex
FROM    #Inv
WHERE   SkipReason IS NULL
ORDER BY InvIndex;

/* ---------------------------------------------------------------------------
   Write
   --------------------------------------------------------------------------- */
DECLARE @IdTimeCard BIT = ISNULL(COLUMNPROPERTY(OBJECT_ID(N'dbo.TimeCard'), N'TimeIndex',     'IsIdentity'), 0);
DECLARE @IdTimeBill BIT = ISNULL(COLUMNPROPERTY(OBJECT_ID(N'dbo.TimeBill'), N'TimeBillIndex', 'IsIdentity'), 0);
DECLARE @IdCostCard BIT = ISNULL(COLUMNPROPERTY(OBJECT_ID(N'dbo.CostCard'), N'CostIndex',     'IsIdentity'), 0);
DECLARE @IdCostBill BIT = ISNULL(COLUMNPROPERTY(OBJECT_ID(N'dbo.CostBill'), N'CostBillIndex', 'IsIdentity'), 0);

DECLARE @nTimeCard INT = 0, @nTimeBill INT = 0, @nCostCard INT = 0, @nCostBill INT = 0;

BEGIN TRY
    BEGIN TRAN;

    /* ---------------- TimeCard ---------------- */
    IF @IdTimeCard = 1 SET IDENTITY_INSERT dbo.TimeCard ON;

    INSERT INTO dbo.TimeCard
          ( TimecardID, TimeIndex, OrigTimeIndex, IsActive, Office, WorkDate, PostDate, Currency, CurrDate,
            Matter, BillMatter, Timekeeper, WorkMattEffDate, BillMattEffDate, TkprEffDate,
            IsNB, ResPct, IsNoCharge, StartTime, TimeInterval, EntryUnitType, EntryUnit,
            WorkHrs, WorkRate, WorkAmt, ChrgCard, OrigHrs, OrigRate, OrigAmt, StdCurrency, StdRate, StdAmt,
            [Language], Narrative, Narrative_UnformattedText, InternalComments,
            TimeType, TransactionType, TaxJurisdiction, Phase, Task, Activity, TaxCode, EditTranType, Disposition,
            ProfMaster, InvMaster, Voucher, OrigCurrency, WorkType, InputTaxCode, PurgeType, WIPRemoveDate,
            RefCurrency, RefRate, RefAmt, UnitCurrRate, FirmCurrRate, UnitCurrRateStd, FirmCurrRateStd,
            WIPHrs, WIPRate, WIPAmt, IsDisplay, LoadNumber, LoadSource, LoadGroup, RateCalcList, GLDate,
            AuthTimekeeper, SpvTimekeeper, IsFlatFeeComplete, IsTaxAdvice, ParTimeIndex, UpdateList,
            TimePracticeArea, MatrixTaxCode, Rpt1CurrRate, Rpt2CurrRate, Rpt3CurrRate,
            Rpt1CurrRateStd, Rpt2CurrRateStd, Rpt3CurrRateStd, PrevProfMaster, Notes, IsTimer, ConversionRefNum,
            ArchetypeCode, CurrProcItemID, LastProcItemID, OrigProcItemID, HasAttachments, [TimeStamp],
            LastPurgeType, Phase2, Task2, Activity2, ProfDetailEdit, ReasonType, ConvSource, ConvGroup,
            TaskID, TaskPermGUID, UniversalTaskID, LxLabel )
    SELECT
            NEWID(), i.TimeIndex, i.TimeIndex, 1, i.Office, i.WorkDate, i.PostDate, i.Currency, i.CurrDate,
            i.Matter, i.Matter, i.Timekeeper, i.MattEffDate, i.MattEffDate, i.TkprEffDate,
            0, NULL, 0, NULL, NULL, NULL, NULL,
            @PlugHours,
            ISNULL(ROUND(i.FeeOrg / NULLIF(@PlugHours, 0), 2), 0),     -- WorkRate
            i.FeeOrg,                                                                     -- WorkAmt
            NULL,
            @PlugHours,
            ISNULL(ROUND(i.FeeOrg / NULLIF(@PlugHours, 0), 2), 0),     -- OrigRate
            i.FeeOrg,                                                                     -- OrigAmt
            i.Currency,
            ISNULL(ROUND(i.FeeOrg / NULLIF(@PlugHours, 0), 2), 0),     -- StdRate
            i.FeeOrg,                                                                     -- StdAmt
            @Language, @Plug, @Plug,
            LEFT(@Plug + N' created ' + CONVERT(NVARCHAR(10), @Today, 120) + N': no time detail existed for invoice ' + i.InvNumber, 510),
            @TimeType, @TimeTranType, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, i.InvIndex, NULL, i.Currency, @WorkType, NULL, NULL, i.PostDate,
            NULL, NULL, NULL, i.UnitCurrRate, i.FirmCurrRate, i.UnitCurrRate, i.FirmCurrRate,
            0, 0, 0, 1, @LoadNumber, @Plug, @Plug, @RateCalcList, i.GLDate,
            NULL, NULL, 0, 0, NULL, NULL,
            NULL, NULL, i.Rpt1CurrRate, i.Rpt2CurrRate, i.Rpt3CurrRate,
            i.Rpt1CurrRate, i.Rpt2CurrRate, i.Rpt3CurrRate, NULL, NULL, 0, LEFT(i.InvNumber, 128),
            @ArchTimeCard, NULL, NULL, NULL, 0, @Today,
            NULL, NULL, NULL, NULL, NULL, NULL, @Plug, @Plug,
            NULL, NULL, NULL, 0
    FROM #Inv i
    WHERE i.NeedFee = 1 AND i.SkipReason IS NULL
    ORDER BY i.TimeIndex;

    SET @nTimeCard = @@ROWCOUNT;
    IF @IdTimeCard = 1 SET IDENTITY_INSERT dbo.TimeCard OFF;

    /* ---------------- TimeBill ---------------- */
    IF @IdTimeBill = 1 SET IDENTITY_INSERT dbo.TimeBill ON;

    INSERT INTO dbo.TimeBill
          ( TimeBillID, TimeBillIndex, ParTimeBill, TimeCard, ARMaster, InvMaster,
            WorkMattEffDate, BillMattEffDate, TkprEffDate, Currency, CurrDate, TaxCode,
            WorkHrs, WorkAmt, StdAmt, BillHrs, BillAmt, IsReversed,
            WorkHrsColl, WorkAmtColl, StdAmtColl, BillHrsWDn, BillHrsWUp, BillHrsColl,
            BillAmtWDn, BillAmtWUp, BillAmtColl, WorkHrsWOff, WorkAmtWOff, StdAmtWOff, BillHrsWOff, BillAmtWOff,
            WorkHrsBal, WorkAmtBal, StdAmtBal, BillHrsBal, BillAmtBal,
            UnitCurrRate, FirmCurrRate, IsAdjustment, ProfAdjustType, WIPHrs, WIPAmt, RefAmt, TaxRate,
            Rpt1CurrRate, Rpt2CurrRate, Rpt3CurrRate, IsAdjCRNote, ArchetypeCode,
            CurrProcItemID, LastProcItemID, OrigProcItemID, HasAttachments, [TimeStamp],
            PurgeType, Disposition, GLDateGlobalChange, GLDate, LxLabel )
    SELECT
            NEWID(), i.TimeBillIndex, NULL, i.TimeIndex, i.ARIndex, i.InvIndex,
            i.MattEffDate, i.MattEffDate, i.TkprEffDate, i.Currency, i.CurrDate, NULL,
            @PlugHours, i.FeeOrg, i.FeeOrg, @PlugHours, i.FeeOrg, 0,
            h.HrsColl, i.FeeColl, i.FeeColl, 0, 0, h.HrsColl,
            0, 0, i.FeeColl, h.HrsWOff, i.FeeWOff, i.FeeWOff, h.HrsWOff, i.FeeWOff,
            h.HrsBal, i.FeeBal, i.FeeBal, h.HrsBal, i.FeeBal,
            i.UnitCurrRate, i.FirmCurrRate, 0, NULL, 0, 0, NULL, NULL,
            i.Rpt1CurrRate, i.Rpt2CurrRate, i.Rpt3CurrRate, 0, @ArchTimeBill,
            NULL, NULL, NULL, 0, @Today,
            NULL, NULL, NULL, i.GLDate, 0
    FROM #Inv i
    CROSS APPLY (SELECT  -- hours prorated the same way as the money; all zero when @PlugHours = 0
                    HrsColl = CASE WHEN i.FeeOrg <> 0 THEN ROUND(@PlugHours * i.FeeColl / i.FeeOrg, 2) ELSE 0 END,
                    HrsWOff = CASE WHEN i.FeeOrg <> 0 THEN ROUND(@PlugHours * i.FeeWOff / i.FeeOrg, 2) ELSE 0 END,
                    HrsBal  = CASE WHEN i.FeeOrg <> 0 THEN ROUND(@PlugHours * i.FeeBal  / i.FeeOrg, 2) ELSE 0 END) h
    WHERE i.NeedFee = 1 AND i.SkipReason IS NULL
    ORDER BY i.TimeBillIndex;

    SET @nTimeBill = @@ROWCOUNT;
    IF @IdTimeBill = 1 SET IDENTITY_INSERT dbo.TimeBill OFF;

    /* ---------------- CostCard ---------------- */
    IF @IdCostCard = 1 SET IDENTITY_INSERT dbo.CostCard ON;

    INSERT INTO dbo.CostCard
          ( CostCardID, CostIndex, OrigCostCard, Office, [Source], WorkDate, PostDate, Currency, CurrDate,
            Matter, BillMatter, Timekeeper, WorkMattEffDate, BillMattEffDate, TkprEffDate,
            IsActive, IsNB, ResPct, IsNoCharge, OrigAmt, ChrgCard, ChargeAmt, EntryUnitType, EntryUnit,
            WorkQty, WorkRate, WorkAmt, StdRate, StdCurrency, StdAmt,
            [Language], Narrative, Narrative_UnformattedText, InternalComments,
            CostType, TransactionType, IsHardCost, TaxJurisdiction, Phase, Task, Activity, TaxCode, EditTranType, Disposition,
            ProfMaster, InvMaster, Voucher, OrigQty, OrigCurrency, OrigRate, InputTaxCode, WorkType, PurgeType, WIPRemoveDate,
            UnitCurrRate, FirmCurrRate, UnitCurrRateStd, FirmCurrRateStd, WIPQty, WIPRate, WIPAmt, IsDisplay,
            LoadNumber, LoadSource, LoadGroup, RateCalcList, RefCurrency, RefRate, RefAmt, IsSummarize, GLDate,
            AuthTimekeeper, SpvTimekeeper, ParCostIndex, MatrixTaxCode,
            Rpt1CurrRate, Rpt2CurrRate, Rpt3CurrRate, Rpt1CurrRateStd, Rpt2CurrRateStd, Rpt3CurrRateStd,
            PrevProfMaster, ConversionRefNum, WHTaxCode, ArchetypeCode,
            CurrProcItemID, LastProcItemID, OrigProcItemID, HasAttachments, [TimeStamp],
            LastPurgeType, Phase2, Task2, Activity2, ProfDetailEdit, ReasonType, WHTaxReason, ConvSource, ConvGroup,
            LxLabel, IsAnticipated, AnticipatedPayee )
    SELECT
            NEWID(), i.CostIndex, i.CostIndex, i.Office, NULL, i.WorkDate, i.PostDate, i.Currency, i.CurrDate,
            i.Matter, i.Matter, i.Timekeeper, i.MattEffDate, i.MattEffDate, i.TkprEffDate,
            1, 0, NULL, 0, i.CostOrg, NULL, NULL, NULL, NULL,
            @PlugQty,
            ISNULL(ROUND(i.CostOrg / NULLIF(@PlugQty, 0), 2), 0),        -- WorkRate
            i.CostOrg,                                                                    -- WorkAmt
            ISNULL(ROUND(i.CostOrg / NULLIF(@PlugQty, 0), 2), 0),        -- StdRate
            i.Currency, i.CostOrg,
            @Language, @Plug, @Plug,
            LEFT(@Plug + N' created ' + CONVERT(NVARCHAR(10), @Today, 120) + N': no cost detail existed for invoice ' + i.InvNumber, 510),
            @CostType, @CostTranType, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, i.InvIndex, NULL, @PlugQty, i.Currency,
            ISNULL(ROUND(i.CostOrg / NULLIF(@PlugQty, 0), 2), 0),        -- OrigRate
            NULL, @WorkType, NULL, i.PostDate,
            i.UnitCurrRate, i.FirmCurrRate, i.UnitCurrRate, i.FirmCurrRate, 0, 0, 0, 1,
            @LoadNumber, @Plug, @Plug, @RateCalcList, NULL, NULL, NULL, 0, i.GLDate,
            NULL, NULL, NULL, NULL,
            i.Rpt1CurrRate, i.Rpt2CurrRate, i.Rpt3CurrRate, i.Rpt1CurrRate, i.Rpt2CurrRate, i.Rpt3CurrRate,
            NULL, LEFT(i.InvNumber, 128), NULL, @ArchCostCard,
            NULL, NULL, NULL, 0, @Today,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, @Plug, @Plug,
            0, 0, NULL
    FROM #Inv i
    WHERE i.NeedCost = 1 AND i.SkipReason IS NULL
    ORDER BY i.CostIndex;

    SET @nCostCard = @@ROWCOUNT;
    IF @IdCostCard = 1 SET IDENTITY_INSERT dbo.CostCard OFF;

    /* ---------------- CostBill ---------------- */
    IF @IdCostBill = 1 SET IDENTITY_INSERT dbo.CostBill ON;

    INSERT INTO dbo.CostBill
          ( CostBillID, CostBillIndex, ParCostBill, CostCard, ARMaster, InvMaster,
            WorkMattEffDate, BillMattEffDate, TkprEffDate, Currency, CurrDate, IsHardCost, TaxCode,
            WorkAmt, BillAmt, IsReversed,
            WorkAmtColl, BillAmtWDn, BillAmtWUp, BillAmtColl, WorkAmtWOff, BillAmtWOff, WorkAmtBal, BillAmtBal,
            UnitCurrRate, FirmCurrRate, IsAdjustment, ProfAdjustType, WIPAmt, RefAmt, TaxRate,
            Rpt1CurrRate, Rpt2CurrRate, Rpt3CurrRate, IsAdjCRNote, ArchetypeCode,
            CurrProcItemID, LastProcItemID, OrigProcItemID, HasAttachments, [TimeStamp],
            PurgeType, Disposition, GLDateGlobalChange, GLDate, LxLabel )
    SELECT
            NEWID(), i.CostBillIndex, NULL, i.CostIndex, i.ARIndex, i.InvIndex,
            i.MattEffDate, i.MattEffDate, i.TkprEffDate, i.Currency, i.CurrDate, 1, NULL,
            i.CostOrg, i.CostOrg, 0,
            i.CostColl, 0, 0, i.CostColl, i.CostWOff, i.CostWOff, i.CostBal, i.CostBal,
            i.UnitCurrRate, i.FirmCurrRate, 0, NULL, 0, NULL, NULL,
            i.Rpt1CurrRate, i.Rpt2CurrRate, i.Rpt3CurrRate, 0, @ArchCostBill,
            NULL, NULL, NULL, 0, @Today,
            NULL, NULL, NULL, i.GLDate, 0
    FROM #Inv i
    WHERE i.NeedCost = 1 AND i.SkipReason IS NULL
    ORDER BY i.CostBillIndex;

    SET @nCostBill = @@ROWCOUNT;
    IF @IdCostBill = 1 SET IDENTITY_INSERT dbo.CostBill OFF;

    /* ---------------- After picture, inside the transaction ---------------- */
    SELECT  Stage       = 'AFTER (inside txn)',
            TimeCards   = @nTimeCard, TimeBills = @nTimeBill,
            CostCards   = @nCostCard, CostBills = @nCostBill,
            FeeStillMissing = (SELECT SUM(im.OrgFee)
                               FROM   dbo.InvMaster im
                               WHERE  NOT EXISTS (SELECT 1 FROM dbo.TimeCard tc WHERE tc.InvMaster = im.InvIndex)
                                 AND  im.IsReversed = 0
                                 AND  im.OrgFee > 0),
            CostStillMissing = (SELECT SUM(ISNULL(im.OrgHCo,0) + CASE WHEN @IncludeSoftCost = 1 THEN ISNULL(im.OrgSCo,0) ELSE 0 END)
                                FROM   dbo.InvMaster im
                                WHERE  NOT EXISTS (SELECT 1 FROM dbo.CostCard cc WHERE cc.InvMaster = im.InvIndex)
                                  AND  im.IsReversed = 0
                                  AND  ISNULL(im.OrgHCo,0) + CASE WHEN @IncludeSoftCost = 1 THEN ISNULL(im.OrgSCo,0) ELSE 0 END > 0);

    IF @nTimeCard <> @nTimeBill OR @nCostCard <> @nCostBill
        THROW 50001, 'Card and bill counts differ; nothing written.', 1;

    IF @Commit = 1
    BEGIN
        COMMIT;
        PRINT 'COMMITTED: ' + CAST(@nTimeCard AS VARCHAR(10)) + ' fee plugs, ' + CAST(@nCostCard AS VARCHAR(10)) + ' cost plugs.';
    END
    ELSE
    BEGIN
        ROLLBACK;
        PRINT 'PREVIEW ONLY - rolled back. Would have written ' + CAST(@nTimeCard AS VARCHAR(10)) + ' fee plugs and '
              + CAST(@nCostCard AS VARCHAR(10)) + ' cost plugs. Set @Commit = 1 to write.';
    END
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    -- IDENTITY_INSERT is session-scoped; make sure none is left on after an error
    IF @IdTimeCard = 1 SET IDENTITY_INSERT dbo.TimeCard OFF;
    IF @IdTimeBill = 1 SET IDENTITY_INSERT dbo.TimeBill OFF;
    IF @IdCostCard = 1 SET IDENTITY_INSERT dbo.CostCard OFF;
    IF @IdCostBill = 1 SET IDENTITY_INSERT dbo.CostBill OFF;
    THROW;
END CATCH;

IF OBJECT_ID('tempdb..#Inv') IS NOT NULL DROP TABLE #Inv;

/* =============================================================================
   UNDO (kept commented; run by hand if a committed batch has to come out)
   Bills first, then cards, matched on the marker fields only - nothing else
   carries 'Plug' in every load/conv field at once.
   =============================================================================
BEGIN TRAN;
DELETE tb FROM dbo.TimeBill tb JOIN dbo.TimeCard tc ON tc.TimeIndex = tb.TimeCard
 WHERE tc.LoadSource = N'Plug' AND tc.ConvSource = N'Plug' AND tc.LoadNumber = N'Plug' AND tc.Narrative = N'Plug';
DELETE FROM dbo.TimeCard
 WHERE LoadSource = N'Plug' AND ConvSource = N'Plug' AND LoadNumber = N'Plug' AND Narrative = N'Plug';
DELETE cb FROM dbo.CostBill cb JOIN dbo.CostCard cc ON cc.CostIndex = cb.CostCard
 WHERE cc.LoadSource = N'Plug' AND cc.ConvSource = N'Plug' AND cc.LoadNumber = N'Plug' AND cc.Narrative = N'Plug';
DELETE FROM dbo.CostCard
 WHERE LoadSource = N'Plug' AND ConvSource = N'Plug' AND LoadNumber = N'Plug' AND Narrative = N'Plug';
-- COMMIT;   -- or ROLLBACK;
*/
