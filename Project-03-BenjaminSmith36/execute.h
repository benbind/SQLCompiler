/*execute.h*/

//
// Project: Execution of queries for SimpleSQL
//
// Benji Smith
// Northwestern University
// CS 211, Winter 2023
//
#pragma once
#include "database.h"
#include "ast.h"

void execute_query(struct Database* db, struct QUERY* query);
//
// #include header files needed to compile function declarations
//

//
// function declarations:
//
