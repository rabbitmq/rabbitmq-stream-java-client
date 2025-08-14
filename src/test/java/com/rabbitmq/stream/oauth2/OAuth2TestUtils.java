// Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// This software, the RabbitMQ Stream Java client library, is dual-licensed under the
// Mozilla Public License 2.0 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.stream.oauth2;

import java.time.Duration;

public final class OAuth2TestUtils {

  private OAuth2TestUtils() {}

  public static String sampleJsonToken(String accessToken, Duration expiresIn) {
    String json =
        "{\n"
            + "  \"access_token\" : \"{accessToken}\",\n"
            + "  \"token_type\" : \"bearer\",\n"
            + "  \"expires_in\" : {expiresIn},\n"
            + "  \"scope\" : \"clients.read emails.write scim.userids password.write idps.write notifications.write oauth.login scim.write critical_notifications.write\",\n"
            + "  \"jti\" : \"18c1b1dfdda04382a8bcc14d077b71dd\"\n"
            + "}";
    return json.replace("{accessToken}", accessToken)
        .replace("{expiresIn}", expiresIn.toSeconds() + "");
  }
}
