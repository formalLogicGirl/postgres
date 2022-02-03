/*-------------------------------------------------------------------------
 *
 * modulecmds.h
 *	  prototypes for modulecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/modulecmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MODULECMDS_H
#define MODULECMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress	CreateModuleCommand(CreateModuleStmt *parsetree,
											const char *queryString,
											int stmt_location, int stmt_len);
extern ObjectAddress AlterModuleRename(AlterModuleRenameStmt *stmt);
extern ObjectAddress AlterModuleOwner(AlterModuleOwnerStmt *stmt);
extern ObjectAddress AlterModuleCreateReplaceFunction(AlterModuleCreateReplaceFuncStmt *smt,
											 const char *queryString,
											 int stmt_location, int stmt_len);
#endif							/* MODULECMDS_H */
