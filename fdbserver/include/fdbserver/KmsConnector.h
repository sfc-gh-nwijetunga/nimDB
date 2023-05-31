/*
 * KmsConnector.h
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

#ifndef KMS_CONNECTOR_H
#define KMS_CONNECTOR_H
#pragma once

#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"

// FDB encryption needs to interact with external Key Management Services (KMS) solutions to lookup/refresh encryption
// keys. KmsConnector interface is an abstract interface enabling implementing specialized KMS connector
// implementations.
// FDB KMSConnector implementation should inherit from KmsConnector and implement pure virtual function,
// EncryptKeyProxyServer instantiates desired implementation object based on SERVER_KNOB->KMS_CONNECTOR_TYPE knob.

class KmsConnector : public NonCopyable {
public:
	KmsConnector(const std::string& conStr) : connectorStr(conStr) {}
	virtual ~KmsConnector() {}

	virtual Future<Void> connectorCore(struct KmsConnectorInterface interf, Reference<const AsyncVar<ServerDBInfo>> db) = 0;

	std::string getConnectorStr() const { return connectorStr; }

protected:
	std::string connectorStr;
	Reference<const AsyncVar<ServerDBInfo>> db;
};

#endif
