/*
 * RESTKmsConnectorTest.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbserver/Knobs.h"
#include "fdbserver/RESTKmsConnector.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct RESTKmsConnectorTestWorkload : TestWorkload {
	static constexpr auto NAME = "RESTKmsConnectorTest";
	Reference<const AsyncVar<ServerDBInfo>> dbInfo;

	RESTKmsConnectorTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), dbInfo(wcx.dbInfo) {}

	Future<Void> setup(Database const& ctx) override { return Void(); }

	ACTOR Future<Void> testWorkload(Database cx) {
		TraceEvent("Nim::hereStart");
		std::vector<std::string> kmsUrls = { "https://127.0.0.1/foo",
			                                 "https://127.0.0.1/foo2",
			                                 "https://127.0.0.1/foo3" };
		wait(updateKMSUrlsKnob(cx, kmsUrls));
		std::unordered_set<std::string> parsedKmsUrls = wait(fetchKMSUrlsFromKnob(cx));
		// for (auto url : parsedKmsUrls) {
		// 	TraceEvent("Nim::here").detail("URL", url);
		// }
		TraceEvent("Nim::hereEnd");
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return testWorkload(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RESTKmsConnectorTestWorkload> RESTKmsConnectorTestWorkloadFactory;
