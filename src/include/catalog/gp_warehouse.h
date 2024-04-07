/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * gp_warehouse.h
 *
 *
 * IDENTIFICATION
 *          src/include/catalog/gp_warehouse.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_WAREHOUSE_H
#define GP_WAREHOUSE_H

#include "catalog/genbki.h"
#include "catalog/gp_warehouse_d.h"

/* ----------------
 *		gp_warehouse definition.  cpp turns this into
 *		typedef struct FormData_gp_warehouse
 * ----------------
 */
CATALOG(gp_warehouse,8690,GpWarehouseRelationId) BKI_SHARED_RELATION
{
    Oid		oid		BKI_FORCE_NOT_NULL;	/* oid */
    Oid     owner BKI_DEFAULT(POSTGRES) BKI_LOOKUP(pg_authid); /* owner of warehouse */
    int32	warehouse_size;				/* warehouse size */
    text	warehouse_name	BKI_FORCE_NOT_NULL;	/* warehouse name */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
    text    status          BKI_FORCE_NOT_NULL; /* status */
    aclitem warehouse_acl[1];       /* access permissions */
#endif
} FormData_gp_warehouse;

typedef FormData_gp_warehouse *Form_gp_warehouse;

DECLARE_UNIQUE_INDEX(gp_warehouse_oid_index, 8086, on gp_warehouse using btree(oid oid_ops));
#define GpWarehouseOidIndexId	8086
DECLARE_UNIQUE_INDEX(gp_warehouse_name_index, 8059, on gp_warehouse using btree(warehouse_name text_ops));
#define GpWarehouseNameIndexId	8059

#endif   /* GP_WAREHOUSE_H */
