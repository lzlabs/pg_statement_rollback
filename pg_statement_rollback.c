/*-------------------------------------------------------------------------
 *
 * pg_statement_rollback.c
 *
 *    Add support to Oracle/DB2 style rollback at statement level in PostgreSQL.
 *
 * Authors: Julien Rouhaud, Dave Sharpe, Gilles Darold 
 * Licence: PostgreSQL
 * Copyright (c) 2020-2023 LzLabs, GmbH
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "commands/portalcmds.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"
#include "tcop/utility.h"
#include "utils/elog.h"
#include "utils/guc.h"
#if PG_VERSION_NUM < 110000
#include "nodes/makefuncs.h"
#include "utils/memutils.h"
#endif

#if PG_VERSION_NUM < 90500
#error Minimum version of PostgreSQL required is 9.5
#endif

/* Define ProcessUtility hook proto/parameters following the PostgreSQL version */
#if PG_VERSION_NUM >= 140000
#define SLR_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
                                       bool readOnlyTree, \
                                        ProcessUtilityContext context, ParamListInfo params, \
                                        QueryEnvironment *queryEnv, DestReceiver *dest, \
                                        QueryCompletion *qc
#define SLR_PROCESSUTILITY_ARGS pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc
#define SLR_PLANNERHOOK_PROTO Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams
#define SLR_PLANNERHOOK_ARGS parse, query_string, cursorOptions, boundParams
#else
#if PG_VERSION_NUM >= 130000
#define SLR_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
					ProcessUtilityContext context, ParamListInfo params, \
					QueryEnvironment *queryEnv, DestReceiver *dest, \
					QueryCompletion *qc
#define SLR_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, qc
#define SLR_PLANNERHOOK_PROTO Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams
#define SLR_PLANNERHOOK_ARGS parse, query_string, cursorOptions, boundParams
#else
#define SLR_PLANNERHOOK_PROTO Query *parse, int cursorOptions, ParamListInfo boundParams
#define SLR_PLANNERHOOK_ARGS parse, cursorOptions, boundParams
#if PG_VERSION_NUM >= 100000
#define SLR_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
					ProcessUtilityContext context, ParamListInfo params, \
					QueryEnvironment *queryEnv, DestReceiver *dest, \
					char *completionTag
#define SLR_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, completionTag
#elif PG_VERSION_NUM >= 90300
#define SLR_PROCESSUTILITY_PROTO Node *parsetree, const char *queryString, \
                                        ProcessUtilityContext context, ParamListInfo params, \
					DestReceiver *dest, char *completionTag
#define SLR_PROCESSUTILITY_ARGS parsetree, queryString, context, params, dest, completionTag
#else
#define SLR_PROCESSUTILITY_PROTO Node *parsetree, const char *queryString, \
                                        ParamListInfo params, bool isTopLevel, \
					DestReceiver *dest, char *completionTag
#define SLR_PROCESSUTILITY_ARGS parsetree, queryString, params, isTopLevel, dest, completionTag
#endif
#endif
#endif

PG_MODULE_MAGIC;

#if PG_VERSION_NUM >= 90500
#define IN_PARALLEL_WORKER (ParallelWorkerNumber >= 0)
#endif

/* Variables to saved hook values in case of unload */
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static emit_log_hook_type prev_log_hook = NULL;

/* Functions used with hooks */
static void slr_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void slr_ExecutorEnd(QueryDesc *queryDesc);
static void slr_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
#if PG_VERSION_NUM >= 90600
                 uint64 count
#else
                 long count
#endif
#if PG_VERSION_NUM >= 100000
                 ,bool execute_once
#endif
	);
static void slr_ExecutorFinish(QueryDesc *queryDesc);
static void slr_ProcessUtility(SLR_PROCESSUTILITY_PROTO);
static PlannedStmt* slr_planner(SLR_PLANNERHOOK_PROTO);
static void disable_differed_slr(ErrorData *edata);

/* Functions */
void	_PG_init(void);
void	_PG_fini(void);
void    slr_save_resowner(void);
void    slr_restore_resowner(void);
void    slr_add_savepoint(void);
void    slr_release_savepoint(void);
static void slr_log(const char *kind);
bool slr_is_write_query(QueryDesc *queryDesc);

#if PG_VERSION_NUM >= 160000
RTEPermissionInfo *localGetRTEPermissionInfo(List *rteperminfos, RangeTblEntry *rte);
#endif

/* Global variables for automatic savepoint */
char    *slr_savepoint_name = "pg_statement_rollback";
bool    slr_enabled        = true;
bool    slr_xact_opened    = false;
bool    slr_pending = false; /* Has an automatic savepoint been created? */
bool    slr_defered_save_resowner = false; /* has defered savepoint */
bool    slr_enable_writeonly = true; /* create savepoint only on write command
					tag (INSERT/DELETE/UPDATE) and DDL */
static int      slr_nest_executor_level = 0;
static bool     slr_planner_done = false;
static int      slr_nest_planner_level = 0;
static ResourceOwner oldresowner = NULL;
static ResourceOwner newresowner = NULL;
static MemoryContext slrPortalContext = NULL;

/*
 * Module load callback
 */
void
_PG_init(void)
{

	/*
	 * Install hooks.
	 */
	prev_planner_hook = planner_hook;
	planner_hook = slr_planner;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = slr_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = slr_ExecutorRun;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = slr_ExecutorEnd;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = slr_ExecutorFinish;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = slr_ProcessUtility;
	prev_log_hook = emit_log_hook;
	emit_log_hook = disable_differed_slr;

	/*
	 * Automatic savepoint
	 *
	 */
	DefineCustomBoolVariable(
		"pg_statement_rollback.enabled",
		"Enable automatic savepoint",
		NULL,
		&slr_enabled,
		true,
		PGC_USERSET,    /* Any user can set it */
		0,
		NULL,           /* No check hook */
		NULL,           /* No assign hook */
		NULL            /* No show hook */
	);

	DefineCustomStringVariable(
		"pg_statement_rollback.savepoint_name",
		"Name of automatic savepoint",
		NULL,
		&slr_savepoint_name,
		"PgSLRAutoSvpt",
		PGC_SUSET,     /* postmaster startup, with the SIGHUP
			        * mechanism, or from the startup packet or SQL if
			        * you're a superuser
				*/
		0,
		NULL,           /* No check hook */
		NULL,           /* No assign hook */
		NULL            /* No show hook */
	);

	DefineCustomBoolVariable(
		"pg_statement_rollback.enable_writeonly",
		"Create savepoint only on write command tag (INSERT/DELETE/UPDATE)"
		" and DDL commands. Call to function with nested write statements"
		" are fully supported.",
		NULL,
		&slr_enable_writeonly,
		true,
		PGC_USERSET,    /* Any user can set it */
		0,
		NULL,           /* No check hook */
		NULL,           /* No assign hook */
		NULL            /* No show hook */
		);
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	planner_hook = prev_planner_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	ProcessUtility_hook = prev_ProcessUtility;
	emit_log_hook = prev_log_hook;

}

/* Keep track that the planner stage is fully terminated */
static PlannedStmt*
slr_planner(SLR_PLANNERHOOK_PROTO)
{
	PlannedStmt *stmt;

	/*
	 * For planners at executor level 0, remember that
	 * we didn't finish the planner stage yet
	 */
	if (slr_nest_executor_level == 0 && slr_nest_planner_level == 0)
		slr_planner_done = false;

	slr_nest_planner_level++;
	elog(DEBUG1, "RSL: increase nest planner level (slr_nest_executor_level %d, slr_nest_planner_level %d, slr_planner_done %d).",
			slr_nest_executor_level, slr_nest_planner_level, slr_planner_done);

	if (prev_planner_hook)
		stmt = prev_planner_hook(SLR_PLANNERHOOK_ARGS);
	else
		stmt = standard_planner(SLR_PLANNERHOOK_ARGS);

	slr_nest_planner_level--;

	/* Remember that the planner stage is now done */
	if (slr_nest_executor_level == 0 && slr_nest_planner_level == 0)
	{
		elog(DEBUG1, "RSL: planner_hook mark planner stage as done.");
		slr_planner_done = true;
	}

	return stmt;
}

static void
slr_ProcessUtility(SLR_PROCESSUTILITY_PROTO)
{
#if PG_VERSION_NUM >= 100000
	Node *parsetree = pstmt->utilityStmt;
#endif
	bool release_add_savepoint = false;
	bool add_savepoint = false;

	/* SPI calls are internal */
	if (dest->mydest == DestSPI
#if PG_VERSION_NUM >= 90500
			|| IN_PARALLEL_WORKER
#endif
		)
	{
		/* do nothing */
	}
	/*
	 * save the current resowner, all caches are associated to it, it'll be
	 * restored after the automatic SAVEPOINT will be created
	 */
	else if (IsA(parsetree, TransactionStmt))
	{
		TransactionStmt *stmt = (TransactionStmt *) parsetree;
		char       *name = NULL;
#if PG_VERSION_NUM < 110000
		ListCell   *cell;
#endif

		/* detect if we are in a transaction or not */
		switch (stmt->kind)
		{
			case TRANS_STMT_PREPARE:
				/* Savepoints do not work with 2PC, so disable automatic
				 * savepoint.  Since a PREPARE TRANSACTION will actually
				 * detach the transaction from the current session, the
				 * transaction is not opened anymore anyway. */
				elog(DEBUG1, "RSL: mark the transaction as closed with PREPARE.");
				slr_xact_opened = false;
				break;
			case TRANS_STMT_BEGIN:
			case TRANS_STMT_START:
				elog( DEBUG1, "RSL: start transaction (slr_nest_executor_level %d, slr_xact_opened %d, kind %d).",
						slr_nest_executor_level, slr_xact_opened, stmt->kind);
				/*
				* we'll need to add a savepoint after the utility execution,
				* but only if this is a top level statement, and we're not
				* already in transaction
				*/
				if (slr_enabled && slr_nest_executor_level == 0 && !slr_xact_opened)
					add_savepoint = true;

				/* mark the transaction as opened in all cases */
				elog(DEBUG1, "RSL: mark the transaction as opened with BEGIN/START.");
				slr_xact_opened = true;
				break;
			case TRANS_STMT_COMMIT:
			case TRANS_STMT_COMMIT_PREPARED:
			case TRANS_STMT_ROLLBACK_PREPARED:
			case TRANS_STMT_ROLLBACK:
				elog(DEBUG1, "RSL: mark the transaction as closed with ROLLBACK.");
				slr_xact_opened = false;
				/* any existing SAVEPOINT will automatically be released */
				slr_pending = false;
				break;
			case TRANS_STMT_SAVEPOINT:
				/* At this point, previous command (either DML or utility) will
				* have opened a SAVEPOINT (if a transaction is opened,
				* otherwise client's SAVEPOINT order will fail anyway).
				*
				* So if client send a SAVEPOINT order, next
				* slr_release_savepoint will release both our savepoint and
				* the client's one, since the client's one will be contained
				* in our own.  We could release our own now, but if the
				* command fails for any reason, the transaction will be
				* irrevocably dead.
				*
				* So our only option is to force adding our own savepoint
				* a second time after user's one if it succeed, but without
				* releasing it before.  This will
				* keep client's savepoint alive and still being able to do our
				* statement rollback.  Unfortunately, it means that we'll have
				* to pile up as many automatic savepoints as the client runs
				* SAVEPOINT commands in its transaction.
				*
				* We will not issue the SAVEPOINT if the client is using the
				* same SAVEPOINT name as our automatic SAVEPOINT/
				*/
#if PG_VERSION_NUM >= 110000
				name = pstrdup(stmt->savepoint_name);
#else
				foreach(cell, stmt->options)
				{
					DefElem    *elem = lfirst(cell);

					if (strcmp(elem->defname, "savepoint_name") == 0)
						name = strVal(elem->arg);
				}
#endif
				if (slr_enabled && name != NULL &&
						strcmp(name, slr_savepoint_name) != 0)
					add_savepoint = true;
				break;
			case TRANS_STMT_RELEASE:
				/* do nothing on RELEASE SAVEPOINT call */
				break;
			case TRANS_STMT_ROLLBACK_TO:
				/* explicit SAVEPOINT handling, do nothing */
				break;
			default:
				elog(ERROR, "RSL: Unexpected transaction kind %d.", stmt->kind);
				break;
		}
	}
	else if (IsA(parsetree, FetchStmt))
	{
		/* do nothing if it's a FETCH */
	}
	else if (slr_enabled && ( IsA(parsetree, DeclareCursorStmt) ||
				IsA(parsetree, PlannedStmt) ) )
	{
		/* The automatic savepoint is required for DECLARE not PLANNED */
		release_add_savepoint = IsA(parsetree, DeclareCursorStmt);

	}
	else if (!IsA(parsetree, ClosePortalStmt))
	{
		/*
		 * release automatic savepoint if any, and create a new one.
		 * We don't check for the planner stage here, since utilities
		 * go straight from parsing to executor without a planner stage.
		 */
		if (slr_enabled && slr_nest_executor_level == 0)
		{
			release_add_savepoint = true;

			/*
			 * Future: if this statement type doesn't really need the automatic
			 * savepoint, add it to the condition above, like ClosePortalStmt.
			 */
			elog(DEBUG1, "RSL: ProcessUtility statement type %d, release and add savepoint.",
					parsetree->type);
		}
	}

	/* Continue the execution of the query */
	slr_nest_executor_level++;

	elog(DEBUG1, "SLR DEBUG: restore ProcessUtility.");
	/* Excecute the utility command, we are not concerned */
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(SLR_PROCESSUTILITY_ARGS);
		else
			standard_ProcessUtility(SLR_PROCESSUTILITY_ARGS);
		slr_nest_executor_level--;
	}
	PG_CATCH();
	{
		slr_nest_executor_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* SPI calls are internal */
	if (dest->mydest == DestSPI
#if PG_VERSION_NUM >= 90500
			|| IN_PARALLEL_WORKER
#endif
			)
	{
		/* do nothing and get out */
		return;
	}

	/*
	 * RELEASE and add a SAVEPOINT if we just executed a statement
	 * that should not rollback on failure of future statement failures
	 */
	if (release_add_savepoint)
	{
		elog(DEBUG1, "RSL: ProcessUtility release and add savepoint (slr_nest_executor_level %d, slr_planner_done %d).",
				slr_nest_executor_level, slr_planner_done);
		release_add_savepoint = false;
		/*
		 * save the current resowner, all caches are associated to it, it'll be
		 * restored after the automatic SAVEPOINT will be created
		 */
		slr_save_resowner();
		/* Release an automatic SAVEPOINT if there's one */
		slr_release_savepoint();
		/* And create a new one */
		slr_add_savepoint();
	}
	/* Add an initial SAVEPOINT if we just opened a transaction */
	else if (add_savepoint)
	{
		elog( DEBUG1, "RSL: ProcessUtility add savepoint (slr_nest_executor_level %d, slr_planner_done %d).",
				slr_nest_executor_level, slr_planner_done);

		/*
		 * save the current resowner, all caches are associated to it, it'll be
		 * restored after the automatic SAVEPOINT will be created
		 */
		slr_save_resowner();

		/* make sure the transaction opening has been processed */
		CommitTransactionCommand();
		CommandCounterIncrement();

		/*
		 * and create our savepoint. We don't check for the planner stage here,
		 * since utilities go straight from parsing to executor without a
		 * planner stage.
		 */
		slr_add_savepoint();

		/* reset the flag to be extra safe */
		add_savepoint = false;
	}
	else if (slr_defered_save_resowner)
	{
		elog(DEBUG1, "RSL: ProcessUtility release and add savepoint (slr_nest_executor_level %d, slr_planner_done %d).",
				slr_nest_executor_level, slr_planner_done);

		/*
		 * save the current resowner, all caches are associated to it, it'll be
		 * restored after the automatic SAVEPOINT will be created
		 */
		slr_save_resowner();

		/* Release an automatic SAVEPOINT if there's one */
		slr_release_savepoint();
		/* And create a new one */
		slr_add_savepoint();

	}

	/* reset defered savepoint */
	slr_defered_save_resowner = false;
}

/*
 * ExecutorStart hook: release automatic savepoint if exists and create a new
 * one.  Be careful though, the planner can spawn multiple level of executors,
 * and we can't interfere with savepoints at that time.  We detect that we
 * passed the planner stage with the planner hook.
 */
static void
slr_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

#if PG_VERSION_NUM >= 90500
	if IN_PARALLEL_WORKER
		return;
#endif

	/*
	 * Only handle savepoints for top level executor that's not spawned by the
	 * planner for insert/update/delete (writer).
	 * After a SELECT function call that have write statement inside we need to
	 * issue a RELEASE+SAVEPOINT. In this case slr_defered_save_resowner have
	 * been set in nested executor level call at bottom of this function.
	 */
	elog(DEBUG1, "RSL: ExecutorStart (slr_nest_executor_level %d, slr_planner_done %d, operation %d).",
			slr_nest_executor_level, slr_planner_done, queryDesc->operation);

	if (slr_enabled && slr_nest_executor_level == 0 && slr_planner_done)
	{
		elog(DEBUG1, "RSL: ExecutorStart save ResourcesOwner.");
		/*
		* save the resowner, all caches are associated to it, it'll be
		* restored just after we define the SAVEPOINT
		*/
		slr_save_resowner();
	}

	/*
	 * if function has write statement we must generate a
	 * release/savepoint after the call to the function.
	 */
	if (slr_enabled && slr_nest_executor_level > 0 && slr_planner_done &&
			slr_enable_writeonly &&
			slr_is_write_query(queryDesc) 
		)
	{
		elog(DEBUG1, "RSL: ExecutorStart enable slr_defered_save_resowner.");
		slr_defered_save_resowner = true;
	}
}

/*
 * ExecutorRun hook: we track nesting depth, and RELEASE / SAVEPOINT for top
 * level executor.
 */
static void
slr_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
#if PG_VERSION_NUM >= 90600
                 uint64 count
#else
                 long count
#endif
#if PG_VERSION_NUM >= 100000
                 , bool execute_once
#endif
	)
{
	elog(DEBUG1, "RSL: ExecutorRun increasing slr_nest_executor_level.");
	slr_nest_executor_level++;

	PG_TRY();
	{
		if (prev_ExecutorRun)
#if PG_VERSION_NUM >= 100000
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			prev_ExecutorRun(queryDesc, direction, count);
#endif
		else
#if PG_VERSION_NUM >= 100000
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			standard_ExecutorRun(queryDesc, direction, count);
#endif
		elog(DEBUG1, "RSL: ExecutorRun decreasing slr_nest_executor_level.");
		slr_nest_executor_level--;
	}
	PG_CATCH();
	{
		elog(DEBUG1, "RSL: ExecutorRun decreasing slr_nest_executor_level.");
		slr_nest_executor_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
slr_ExecutorFinish(QueryDesc *queryDesc)
{

	elog(DEBUG1, "RSL: ExecutorFinish increasing slr_nest_executor_level.");
	slr_nest_executor_level++;

	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		slr_nest_executor_level--;
		elog(DEBUG1, "RSL: ExecutorFinish decreasing slr_nest_executor_level.");
	}
	PG_CATCH();
	{
		slr_nest_executor_level--;
		elog(DEBUG1, "RSL: ExecutorFinish decreasing slr_nest_executor_level.");
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/* ExecutorEnd hook: for write statements, release automatic savepoint and
 * create a new one.
 * Be careful though, the planner can spawn multiple level of executors,
 * and we can't interfere with savepoints at that time.  We detect that we
 * passed the planner stage with the planner hook.
 */
static void
slr_ExecutorEnd(QueryDesc *queryDesc)
{
	/*
	 * Only handle automatic savepoints for top level executor that's not
	 * spawned by the planner for write SQL (like slr_ExecutorStart()).
	 */
	elog( DEBUG1, "RSL: ExecutorEnd (slr_nest_executor_level %d, slr_planner_done %d, operation %d).",
			slr_nest_executor_level, slr_planner_done, queryDesc->operation);

	if (
#if PG_VERSION_NUM >= 90500
		!IN_PARALLEL_WORKER &&
#endif
		slr_enabled && slr_nest_executor_level == 0 && slr_planner_done && (
				!slr_enable_writeonly ||
			 	slr_defered_save_resowner ||
			 	slr_is_write_query(queryDesc) 
			 )
		)
	{
		/* Release an automatic SAVEPOINT if there's one */
		slr_release_savepoint();
		/* And create a new one */
		slr_add_savepoint();

		slr_defered_save_resowner = false;
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * This function set the current resowner to the one that has been created by
 * DefineSavepoint(), after it has been overriden by slr_save_resowner')
 */
void
slr_restore_resowner(void)
{
	Assert(slr_nest_executor_level == 0);

	if (slr_enabled && slr_xact_opened && newresowner != NULL)
	{
		CurrentResourceOwner = newresowner;
		newresowner = NULL;

		elog(DEBUG1, "RSL: restoring Resource owner.");
		slr_log("SAVEPOINT");
	}
}

/*
 * This function save the current resowner, that'll be overriden by
 * DefineSavepoint)
 */
void
slr_save_resowner(void)
{
	Assert(oldresowner == NULL);
	Assert(slr_nest_executor_level == 0);

	if (slr_enabled && slr_xact_opened)
	{
		oldresowner = CurrentResourceOwner;
		elog(DEBUG1, "RSL: Saving the Resource owner.");
		slrPortalContext = PortalContext;
	}
}

/*
 * This function create a new savepoint.  However, adding a SAVEPOINT will
 * create a new resowner, and we can't let the new resowner at this point,
 * because following query execution will have to clear all entries associated
 * to the former resowner.  Therefore, we backup the new resowner, which will
 * be restored after the cleanup is done.  There's no hook available to do
 * that, so we rely on the current query context (PortalContext) cleanup
 * callback to do this.
 */
void
slr_add_savepoint(void)
{
	Assert(slr_nest_executor_level == 0);

	if (slr_enabled && slr_xact_opened)
	{
		MemoryContextCallback *slr_cb = NULL;

		elog(DEBUG1, "RSL: adding savepoint %s.", slr_savepoint_name);

		/* Define savepoint */
		DefineSavepoint(slr_savepoint_name);
		elog(DEBUG1, "RSL: CommitTransactionCommand.");
		CommitTransactionCommand();
		elog(DEBUG1, "RSL: CommandCounterIncrement.");
		CommandCounterIncrement();

		/*
		 * Backup the new resowner, will be restore the end of execution on the
		 * Portal memory context callback
		 */
		newresowner = CurrentResourceOwner;

		/* And restore the one we previously saved */
		if (oldresowner == NULL)
			elog(ERROR, "Automatic savepoint internal error, no resource owner.");
		if (slrPortalContext == NULL)
			elog(ERROR, "Automatic savepoint internal error, no portal context.");

		CurrentResourceOwner = oldresowner;
		oldresowner = NULL;

		/*
		 * Add the callback that will restore the new resowner when the cleanup
		 * will be finished
		 */
		slr_cb = MemoryContextAlloc(slrPortalContext, sizeof(MemoryContextCallback));
		slr_cb->arg = NULL;
		slr_cb->func = (void *) slr_restore_resowner;
		elog(DEBUG1, "RSL: add the callback that will restore the new resowner when the cleanup.");
		MemoryContextRegisterResetCallback(slrPortalContext, slr_cb);
		slrPortalContext = NULL;

		slr_pending = true;
	}
}

/*
 * This function release an automatic SAVEPOINT that
 * has previously been created
 */
void
slr_release_savepoint(void)
{
	Assert(slr_nest_executor_level == 0);

	if (slr_enabled && slr_xact_opened && slr_pending)
	{
#if PG_VERSION_NUM < 110000
		List       *options = NIL;
		DefElem    *elem = NULL;

		elog(DEBUG1, "RSL: releasing savepoint %s.", slr_savepoint_name);

		elem = makeDefElem("savepoint_name",
				(Node *) makeString(slr_savepoint_name)
#if PG_VERSION_NUM >= 100000
				, -1
#endif
				);

		options = list_make1(elem);

		ReleaseSavepoint(options);
#else
		ReleaseSavepoint(slr_savepoint_name);
#endif
		CommitTransactionCommand();
		CommandCounterIncrement();

		slr_pending = false;

		/* Manually log the order if needed */
		slr_log("RELEASE");
	}
}

static void
slr_log(const char *kind)
{
	bool was_logged = false;

	/* transaction stmt are only logged for log_statement = ALL */
	if (LOGSTMT_ALL <= log_statement)
	{
		ereport(LOG, (errmsg("statement: %s %s; /* automatic savepoint */",
					kind, slr_savepoint_name),
					errhidestmt(true)));
		was_logged = true;
	}

	/*
	 * If log_duration and log_min_duration_statement is st to 0, always log
	 * these queries. We don't compute the actual duration for now, but it could
	 * be added if needed.  The main problem to compute duration is that the
	 * SAVEPOINT creation is actually done in two steps, which makes the timing
	 * not really meaningful.  Instead, display a "0.01" as duration.
	 */
	if (log_duration || log_min_duration_statement == 0)
	{
		if (was_logged)
			ereport(LOG,
					(errmsg("duration: %s ms", "0.01"),
						errhidestmt(true)));
		else
			ereport(LOG,
					(errmsg("duration: %s ms  statement: %s %s; /* automatic savepoint */",
						"0.01", kind, slr_savepoint_name),
						errhidestmt(true)));
	}
}

/*
 * Check that the query does not imply any writes to any tables.
 */
bool
slr_is_write_query(QueryDesc *queryDesc)
{
	ListCell   *l;

	/*
	 * Fail if write permissions are requested in parallel mode for table
	 * (temp or non-temp), otherwise fail for any non-temp table.
	 */
	foreach(l, queryDesc->plannedstmt->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

		if (rte->rtekind != RTE_RELATION)
			continue;

#if PG_VERSION_NUM < 160000
		if ((rte->requiredPerms & (~ACL_SELECT)) == 0)
			continue;
#else
		if (rte->perminfoindex != 0)
		{
			RTEPermissionInfo *perminfo = localGetRTEPermissionInfo(queryDesc->estate->es_rteperminfos, rte);
			if ((perminfo->requiredPerms & (~ACL_SELECT)) == 0)
				continue;
		}
#endif

		return true;
	}

	return false;
}

static void
disable_differed_slr(ErrorData *edata)
{
	/* Do not ask for automatic savepoint if previous statement has an error */
	if (edata->elevel >= ERROR)
		slr_defered_save_resowner = false;

	/* Continue chain to previous hook */
	if (prev_log_hook)
		(*prev_log_hook) (edata);
}

#if PG_VERSION_NUM >= 160000
/*
 * getRTEPermissionInfo
 *              Find RTEPermissionInfo for a given relation in the provided list.
 *
 * This is a simple list_nth() operation, though it's good to have the
 * function for the various sanity checks.
 */
RTEPermissionInfo *
localGetRTEPermissionInfo(List *rteperminfos, RangeTblEntry *rte)
{
	RTEPermissionInfo *perminfo;

	if (rte->perminfoindex == 0 ||
		rte->perminfoindex > list_length(rteperminfos))
		elog(ERROR, "invalid perminfoindex %u in RTE with relid %u",
			 rte->perminfoindex, rte->relid);
	perminfo = list_nth_node(RTEPermissionInfo, rteperminfos,
							 rte->perminfoindex - 1);
	if (perminfo->relid != rte->relid)
		elog(ERROR, "permission info at index %u (with relid=%u) does not match provided RTE (with relid=%u)",
			 rte->perminfoindex, perminfo->relid, rte->relid);

	return perminfo;
}
#endif
