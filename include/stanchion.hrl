%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

-record(context, {auth_bypass :: boolean(),
                  bucket :: binary(),
                  owner_id :: all | string()}).

-record(moss_user_v1, {
          name :: string(),
          display_name :: string(),
          email :: string(),
          key_id :: string(),
          key_secret :: string(),
          canonical_id :: string(),
          buckets=[] :: [moss_bucket()]}).

-record(rcs_user_v2, {
          name :: string(),
          display_name :: string(),
          email :: string(),
          key_id :: string(),
          key_secret :: string(),
          canonical_id :: string(),
          buckets=[] :: [moss_bucket()],
          status=enabled :: enabled | disabled}).
-type moss_user() :: #rcs_user_v2{} | #moss_user_v1{}.
-type rcs_user() :: #rcs_user_v2{} | #moss_user_v1{}.

-record(moss_bucket_v1, {
          name :: string(),
          last_action :: created | deleted,
          creation_date :: string(),
          modification_time :: erlang:timestamp(),
          acl :: acl()}).

-type moss_bucket() :: #moss_bucket_v1{}.

-type acl_perm() :: 'READ' | 'WRITE' | 'READ_ACP' | 'WRITE_ACP' | 'FULL_CONTROL'.
-type acl_perms() :: [acl_perm()].
-type acl_grant() :: {{string(), string()}, acl_perms()} | {atom(), acl_perms()}.
-type acl_owner() :: {string(), string()} | {string(), string(), string()}.
-record(acl_v1, {owner={"", ""} :: acl_owner(),
                 grants=[] :: [acl_grant()],
                 creation_time=now() :: erlang:timestamp()}).
-record(acl_v2, {owner={"", "", ""} :: acl_owner(),
                 grants=[] :: [acl_grant()],
                 creation_time=now() :: erlang:timestamp()}).
-type acl1() :: #acl_v1{}.
-type acl2() :: #acl_v2{}.
-type acl() :: acl1() | acl2().

-record(access_v1, {
          method :: atom(), % PUT / GET / POST / ....
          target :: atom(), % object | object_acl | ....
          id :: binary(),
          bucket :: binary(),
          key = <<>> :: binary(),
          req %:: #wm_reqdata{} % request of webmachine
         }).

-type access() :: #access_v1{}.

-define(ACL, #acl_v2).
-define(USER_BUCKET, <<"moss.users">>).
-define(BUCKETS_BUCKET, <<"moss.buckets">>).
-define(FREE_BUCKET_MARKER, <<"0">>).
-define(MOSS_USER, #rcs_user_v2).
-define(RCS_USER, #rcs_user_v2).
-define(MD_ACL, <<"X-Moss-Acl">>).
-define(MD_POLICY, <<"X-Rcs-Policy">>).
