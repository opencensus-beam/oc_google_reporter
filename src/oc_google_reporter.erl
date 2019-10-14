%%%------------------------------------------------------------------------
%% Copyright 2017, OpenCensus Authors
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc Reporter for sending trace spans to Google Stackdriver Trace.
%% @end
%%%-----------------------------------------------------------------------
-module(oc_google_reporter).

-export([init/1,
         report/2]).

-include_lib("opencensus/include/opencensus.hrl").

-define(TRACE_URL(ProjectId),
       <<"https://cloudtrace.googleapis.com/v1/projects/", ProjectId/binary, "/traces">>).

init(Configuration) ->
    case maps:find(project_id, Configuration) of
        {ok, ProjectId} ->
            Source = maps:get(credentials_source, Configuration, default),
            #{credentials => Source,
              project_id => ProjectId};
        error ->
            error_logger:info_msg("no reporter: project_id missing from oc_google_reporter configuration"),
            ignore
    end.

report(Spans, Opts) ->
    ProjectId = maps:get(project_id, Opts),
    Source = maps:get(credentials, Opts),
    {ok, Creds} = augle:creds_from(Source),
    Headers = augle:headers(Creds),
    PatchBody = jsx:encode(#{traces => format_spans(ProjectId, Spans)}),
    case hackney:patch(?TRACE_URL(ProjectId), Headers, PatchBody, []) of
        {ok, Status, _Headers, Ref} when Status =:= 204
                                       ; Status =:= 200 ->
            {ok, _Body} = hackney:body(Ref),
            ok;
        {ok, _Status, _Headers, Ref} ->
            {ok, ErrorJson} = hackney:body(Ref),
            Error = jsx:decode(ErrorJson, [return_maps]),
            error_logger:info_msg("failed to upload spans to google trace: ~p", [Error]),
            %% try again with a backoff?
            ok;
        {error, timeout} ->
            %% try again with a backoff?
            ok;
        {error, _Reason} ->
            %% try again with a backoff?
            ok
    end.

format_spans(ProjectId, Spans) when is_list(Spans) ->
    [format_span(ProjectId, Span) || Span <- Spans].

format_span(ProjectId, #span{name=Name,
                             trace_id=TraceId,
                             span_id=SpanId,
                             parent_span_id=ParentSpanId,
                             start_time=StartTime,
                             end_time=EndTime,
                             attributes=Attributes}) ->
    EncodedTraceId = list_to_binary(io_lib:format("~32.16.0b", [TraceId])),
    EncodedSpanId = integer_to_binary(SpanId),
    Span = case ParentSpanId of
               undefined ->
                   #{spanId => EncodedSpanId,
                     name => term_to_string(Name),
                     startTime => wts:rfc3339(StartTime),
                     endTime => wts:rfc3339(EndTime),
                     labels => Attributes
                    };
               _ ->
                   EncodedParentSpanId = integer_to_binary(ParentSpanId),
                   #{spanId => EncodedSpanId,
                     name => term_to_string(Name),
                     startTime => wts:rfc3339(StartTime),
                     endTime => wts:rfc3339(EndTime),
                     parentSpanId => EncodedParentSpanId,
                     labels => Attributes
                    }
           end,

    #{projectId => ProjectId,
      traceId => EncodedTraceId,
      spans => [Span]}.


term_to_string(Term) when is_binary(Term) ->
    Term;
term_to_string(Term) ->
    list_to_binary(io_lib:format("~p", [Term])).
