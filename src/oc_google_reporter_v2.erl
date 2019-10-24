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
%% @doc Reporter for sending trace spans v2 Google Stackdriver Trace.
%% @end
%%%-----------------------------------------------------------------------
-module(oc_google_reporter_v2).

-export([init/1,
         report/2]).

-include_lib("opencensus/include/opencensus.hrl").

-define(TRACE_URL(ProjectId),
        <<"https://cloudtrace.googleapis.com/v2/projects/", ProjectId/binary, "/traces:batchWrite">>).

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
    PatchBody = jsx:encode(#{spans => format_spans(ProjectId, Spans)}),
    case hackney:post(?TRACE_URL(ProjectId), Headers, PatchBody, []) of
        {ok, Status, _Headers, _Client} when Status =:= 204
                                           ; Status =:= 200 ->
            ok;
        {ok, _Status, RespHeaders, Client} ->
            {ok, ErrorBody} = hackney:body(Client),
            case lists:keyfind(<<"Content-Type">>, 1, RespHeaders) of
                {_, <<"application/json", _/binary>>} ->
                    Error = jsx:decode(ErrorBody, [return_maps]),
                    error_logger:info_msg("failed to upload spans to google trace: ~p", [Error]);
                _ ->
                    error_logger:info_msg("failed to upload spans to google trace: ~s", [ErrorBody])
            end,
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
    [span_to_map(ProjectId, Span) || Span <- Spans].

trunc_string(V) ->
    #{<<"value">> => V,
      <<"truncatedByteCount">> => 0}.

format_attribute_map(_K, V) when is_integer(V) -> #{<<"int_value">> => V};
format_attribute_map(_K, V) when is_boolean(V) -> #{<<"bool_value">> => V};
format_attribute_map(_K, V) -> #{<<"string_value">> => trunc_string(V)}.

to_map(_, undefined) -> undefined;
to_map(attributes, AttributeMap) ->
    {TruncatedAttributeMap, Dropped} = drop(maps:to_list(AttributeMap), 32),
    AttributeMap = maps:from_list(TruncatedAttributeMap),
    #{<<"attribute_map">> => maps:map(fun format_attribute_map/2, AttributeMap),
      <<"dropped_attributes_count">> => Dropped};
to_map(annotation, #annotation{description=Description,
                               attributes=Attributes}) ->
    #{<<"description">> => trunc_string(Description),
      <<"attributes">> => to_map(attributes, Attributes)};
to_map(link, #link{trace_id=TraceId,
                   span_id=SpanId,
                   type=Type,
                   attributes=Attributes}) ->
    #{<<"trace_id">> => TraceId,
      <<"span_id">> => SpanId,
      <<"type">> => Type,
      <<"attributes">> => to_map(attributes, Attributes)};
to_map(links, LinkList) ->
    Dropped = 0,
    #{<<"link">> => [to_map(link, L) || L <- LinkList],
      <<"dropped_links_count">> => Dropped};
to_map(stack_trace, Frames) ->
    #{<<"stack_frames">> => to_map(stack_frames, Frames),
      <<"stack_trace_hash_id">> => 0
     };
to_map(stack_frame, {Module, Function, _Arity, Location}) ->
    FileName = proplists:get_value(file, Location, undefined),
    Line = proplists:get_value(line, Location, 0),
    #{<<"function_name">> => Function,
      <<"original_function_name">> => Function,
      <<"file_name">> => FileName,
      <<"line_number">> => Line,
      <<"column_number">> => undefined,
      <<"load_module">> => #{<<"module">> => trunc_string(Module),
                             <<"build_id">> => undefined},
      <<"source_version">> => undefined};
to_map(stack_frames, Frames) ->
    #{<<"frame">> => [to_map(stack_frame, Frame) || Frame <- Frames],
      <<"dropped_frames_count">> => 0
     };
to_map(time_events, TimeEventList) ->
    {Annotations, MessageEvents} =
        lists:foldl(fun({Time, Annotation=#annotation{}}, {AnnotationAcc, MessageEventAcc}) ->
                            {[#{<<"time">> => wts:rfc3339(Time),
                                <<"annotation">> => to_map(annotation, Annotation)} | AnnotationAcc], MessageEventAcc};
                       ({Time, MessageEvent=#message_event{}}, {AnnotationAcc, MessageEventAcc}) ->
                            {AnnotationAcc, [#{<<"time">> => wts:rfc3339(Time),
                                               <<"message_event">> => to_map(message_event, MessageEvent)} | MessageEventAcc]}
                    end, {[], []}, TimeEventList),
    {AnnotationsList, DroppedAnnotations} = drop(Annotations, 32),
    {MessageEventsList, DroppedMessageEvents} = drop(MessageEvents, 128),
    #{<<"time_event">> => AnnotationsList ++ MessageEventsList,
      <<"dropped_annotations_count">> => DroppedAnnotations,
      <<"dropped_message_events_count">> => DroppedMessageEvents};
to_map(message_event, #message_event{type=Type,
                                     id=Id,
                                     uncompressed_size=UncompressedSize,
                                     compressed_size=CompressedSize}) ->
    #{<<"type">> => Type,
      <<"id">> => Id,
      <<"uncompressed_size">> => UncompressedSize,
      <<"compressed_size">> => CompressedSize};
to_map(status, #status{code=Code,
                       message=Message}) ->
    #{<<"code">> => Code,
      <<"message">> => Message,
      <<"details">> => []
     }.

span_to_map(ProjectId, #span{name=DisplayName,
                             trace_id=TraceId,
                             span_id=SpanId,
                             parent_span_id=ParentSpanId,
                             start_time=StartTime,
                             end_time=EndTime,
                             attributes=Attributes,
                             links=Links,
                             stack_trace=StackTrace,
                             time_events=TimeEvents,
                             status=Status,
                             same_process_as_parent_span=SameProcessAsParentSpan,
                             child_span_count=ChildSpanCount}) ->
    EncodedTraceId  = list_to_binary(io_lib:format("~32.16.0b", [TraceId])),
    EncodedSpanId  = list_to_binary(io_lib:format("~16.16.0b", [SpanId])),
    SpanName = <<"projects/", ProjectId/binary, "/traces/", EncodedTraceId/binary, "/spans/", EncodedSpanId/binary>>,
    EncodedParentSpanId = case ParentSpanId of undefined -> <<>>; _ -> list_to_binary(io_lib:format("~16.16.0b", [ParentSpanId])) end,
    Span = #{<<"name">> => SpanName,
             <<"spanId">> => EncodedSpanId,
             <<"parentSpanId">> => EncodedParentSpanId,
             <<"displayName">> => trunc_string(DisplayName),
             <<"startTime">> => wts:rfc3339(StartTime),
             <<"endTime">> => wts:rfc3339(EndTime),
             <<"attributes">> => to_map(attributes, Attributes),
             <<"stackTrace">> => case StackTrace of undefined -> to_map(stack_trace, []); _ -> to_map(stack_trace, StackTrace) end,
             <<"timeEvents">> => to_map(time_events, TimeEvents),
             <<"links">> => to_map(links, Links)},

    %% optional fields
    lists:foldl(fun({_, undefined}, Acc) ->
                        Acc;
                   ({<<"status">>, S}, Acc) ->
                        Acc#{<<"status">> => to_map(status, S)};
                   ({K, V}, Acc) ->
                        Acc#{K => V}
                end, Span, [{<<"status">>, Status},
                            {<<"sameProcessAsParentSpan">>, SameProcessAsParentSpan},
                            {<<"childSpanCount">>, ChildSpanCount}]).

drop(List, Limit) ->
    case erlang:length(List) of
        Size when Size =< Limit ->
            {List, 0};
        Size ->
            {lists:sublist(List, Limit), Size - Limit}
    end.

