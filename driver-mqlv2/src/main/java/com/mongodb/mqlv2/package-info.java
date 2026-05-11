/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Experimental MQLv2 driver API: AST taxonomy, serializer, and {@code Pipeline} wrapper
 * implementing {@code Mqlv2Source} so AST constructions can be passed directly to
 * {@code MongoDatabase.mqlv2(Mqlv2Source)}.
 */
package com.mongodb.mqlv2;
