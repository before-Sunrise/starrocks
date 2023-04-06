// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Get meta info with privilege checking
 */
public class MetaInfoService {

    public static class GetDatabasesAction extends RestBaseAction {

        public GetDatabasesAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            GetDatabasesAction action = new GetDatabasesAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/meta/_databases", action);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
            checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.SHOW);

            List<String> dbNames = GlobalStateMgr.getCurrentState().getDbNames();
            List<String> dbNameSet = Lists.newArrayList();

            Map<String, Object> resultMap = new HashMap<>(4);
            try {
                for (String fullName : dbNames) {
                    final String db = ClusterNamespace.getNameFromFullName(fullName);
                    if (!GlobalStateMgr.getCurrentState().getAuth()
                            .checkDbPriv(ConnectContext.get(), db, PrivPredicate.SHOW)) {
                        continue;
                    }
                    dbNameSet.add(db);
                }

                Collections.sort(dbNameSet);
                // handle limit offset
                Pair<Integer, Integer> fromToIndex = getFromToIndex(request, dbNameSet.size());
                // to json response
                List<String> resultDbNameSet = dbNameSet.subList(fromToIndex.first, fromToIndex.second);
                resultMap.put("status", 200);
                resultMap.put("databases", resultDbNameSet);
                resultMap.put("count", resultDbNameSet.size());
            } catch (StarRocksHttpException e) {
                resultMap.put("status", e.getCode().code());
                resultMap.put("exception", e.getMessage());
            }

            try {
                ObjectMapper mapper = new ObjectMapper();
                String result = mapper.writeValueAsString(resultMap);
                response.setContentType("application/json");
                response.getContent().append(result);
                sendResult(request, response,
                        HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
            } catch (Exception e) {
                response.getContent().append(e.getMessage());
                sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
        }
    }

    public static class GetTablesAction extends RestBaseAction {

        public GetTablesAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            GetTablesAction action = new GetTablesAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/meta/{" + DB_KEY + "}/_tables", action);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
            String dbName = request.getSingleParameter(DB_KEY);

            Map<String, Object> resultMap = new HashMap<>(4);

            try {
                if (Strings.isNullOrEmpty(dbName)) {
                    throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "{database} must be selected");
                }
                checkDbAuth(ConnectContext.get().getCurrentUserIdentity(), dbName, PrivPredicate.SHOW);
                Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
                if (db == null) {
                    throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                            "Database [" + dbName + "] " + "does not exists");
                }
                List<String> tblNames = Lists.newArrayList();
                for (Table tbl : db.getTables()) {
                    if (!GlobalStateMgr.getCurrentState().getAuth()
                            .checkTblPriv(ConnectContext.get(), dbName, tbl.getName(), PrivPredicate.SHOW)) {
                        continue;
                    }
                    tblNames.add(tbl.getName());
                }

                Collections.sort(tblNames);

                // handle limit offset
                Pair<Integer, Integer> fromToIndex = getFromToIndex(request, tblNames.size());
                List<String> resultTblNames = tblNames.subList(fromToIndex.first, fromToIndex.second);
                resultMap.put("status", 200);
                resultMap.put("database", dbName);
                resultMap.put("tables", resultTblNames);
                resultMap.put("table count", resultTblNames.size());
            } catch (StarRocksHttpException e) {
                resultMap.put("status", e.getCode().code());
                resultMap.put("exception", e.getMessage());
            }

            ObjectMapper mapper = new ObjectMapper();
            try {
                String result = mapper.writeValueAsString(resultMap);
                response.setContentType("application/json");
                response.getContent().append(result);
                sendResult(request, response,
                        HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
            } catch (Exception e) {
                response.getContent().append(e.getMessage());
                sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    // get limit and offset from query parameter
    // and return fromIndex and toIndex of a list
    private static Pair<Integer, Integer> getFromToIndex(BaseRequest request, int maxNum) {
        String limitStr = request.getSingleParameter("limit");
        String offsetStr = request.getSingleParameter("offset");

        int offset = 0;
        int limit = Integer.MAX_VALUE;

        limit = (limitStr != null && !limitStr.isEmpty()) ? Integer.parseInt(limitStr) : limit;
        offset = (offsetStr != null && !offsetStr.isEmpty()) ? Integer.parseInt(offsetStr) : offset;

        if (limit < 0) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Param limit should >= 0");
        }
        if (offset < 0) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Param offset should >= 0");
        }
        if (offset > 0 && limit == Integer.MAX_VALUE) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "Param offset should be set with param limit");
        }

        if (maxNum <= 0) {
            return Pair.create(0, 0);
        }
        return Pair.create(Math.min(offset, maxNum - 1), Math.min(limit + offset, maxNum));
    }
}
