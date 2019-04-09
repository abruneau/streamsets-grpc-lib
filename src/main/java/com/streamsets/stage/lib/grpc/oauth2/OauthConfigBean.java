/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.stage.lib.grpc.oauth2;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.ValueChooserModel;

public class OauthConfigBean {
  private static final int DISPLAY_POSITION_OFFSET = 1000;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.BOOLEAN,
          defaultValue = "false",
          label = "Use Oauth",
          description = "Enable Oauth credential",
          group = "#0",
          displayPosition = DISPLAY_POSITION_OFFSET + 0
  )
  public boolean oauthEnabled;


  @ConfigDef(
          required = true,
          type = ConfigDef.Type.MODEL,
          label = "Credentials Case",
          dependencies = {
                  @Dependency(configName = "oauthEnabled", triggeredByValues = "true"),
          },
          group = "#0",
          displayPosition = DISPLAY_POSITION_OFFSET + 10
  )
  @ValueChooserModel(CredentialsCaseChooserValues.class)
  public CredentialsCase credentialsCase = CredentialsCase.ACCESS_TOKEN_CREDENTIALS;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "Access Token",
          dependencies = {
                  @Dependency(configName = "credentialsCase", triggeredByValues = "ACCESS_TOKEN_CREDENTIALS"),
          },
          group = "#0",
          displayPosition = DISPLAY_POSITION_OFFSET + 20
  )
  public String accessToken;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "Oauth Client Id",
          dependencies = {
                  @Dependency(configName = "credentialsCase", triggeredByValues = "REFRESH_TOKEN_CREDENTIALS"),
          },
          group = "#0",
          displayPosition = DISPLAY_POSITION_OFFSET + 30
  )
  public String oauthClientId;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "Oauth Client Secret",
          dependencies = {
                  @Dependency(configName = "credentialsCase", triggeredByValues = "REFRESH_TOKEN_CREDENTIALS"),
          },
          group = "#0",
          displayPosition = DISPLAY_POSITION_OFFSET + 40
  )
  public String oauthClientSecret;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "Oauth Client",
          dependencies = {
                  @Dependency(configName = "credentialsCase", triggeredByValues = "REFRESH_TOKEN_CREDENTIALS"),
          },
          group = "#0",
          displayPosition = DISPLAY_POSITION_OFFSET + 50
  )
  public String refreshToken;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "Exchange Url",
          dependencies = {
                  @Dependency(configName = "credentialsCase", triggeredByValues = "REFRESH_TOKEN_CREDENTIALS"),
          },
          group = "#0",
          displayPosition = DISPLAY_POSITION_OFFSET + 60
  )
  public String exchangeUrl;

  /** Returns a set of {@link Credentials} which can be used to authenticate requests. */
  public Credentials getCredentials() {
    if (credentialsCase == CredentialsCase.ACCESS_TOKEN_CREDENTIALS) {
      return createAccessTokenCredentials();
    } else if (credentialsCase == CredentialsCase.REFRESH_TOKEN_CREDENTIALS) {
      return createRefreshTokenCredentials();
    } else {
      throw new IllegalArgumentException(
              "Unknown oauth credential type: " + credentialsCase);
    }
  }

  private Credentials createAccessTokenCredentials() {
    AccessToken oauthAccessToken = new AccessToken(accessToken, null);
    return OAuth2Credentials.create(oauthAccessToken);
  }

  private Credentials createRefreshTokenCredentials() {
    return RefreshTokenCredentials.create(oauthClientId, oauthClientSecret, refreshToken, exchangeUrl);
  }
}
