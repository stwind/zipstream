-module(zipstream).

-include_lib("kernel/include/file.hrl").

-export([
        open/0,
        write_file/3,
        write_file_start/4,
        write_file_data/2,
        write_file_end/1,
        close/1
    ]).

-record(local_file_header, {
        version_needed,
        gp_flag,
        comp_method,
        last_mod_time,
        last_mod_date,
        crc32,
        comp_size,
        uncomp_size,
        file_name_length,
        extra_field_length}).

-record(eocd, {
        disk_num,
        start_disk_num,
        entries_on_disk,
        entries,
        size,
        offset,
        zip_comment_length}).

-record(cd_file_header, {
        version_made_by,
        version_needed,
        gp_flag,
        comp_method,
        last_mod_time,
        last_mod_date,
        crc32,
        comp_size,
        uncomp_size,
        file_name_length,
        extra_field_length,
        file_comment_length,
        disk_num_start,
        internal_attr,
        external_attr,
        local_header_offset}).

-define(STORED, 0).

-define(LOCAL_FILE_MAGIC,16#04034b50).
-define(LOCAL_FILE_HEADER_SZ,(4+2+2+2+2+2+4+4+4+2+2)).
-define(LOCAL_FILE_HEADER_CRC32_OFFSET, 4+2+2+2+2+2).
-define(CENTRAL_FILE_MAGIC, 16#02014b50).
-define(CENTRAL_FILE_HEADER_SZ,(4+2+2+2+2+2+2+4+4+4+2+2+2+2+2+4+4)).
-define(END_OF_CENTRAL_DIR_MAGIC, 16#06054b50).

-type out() :: {integer(), binary()}.
-type lh() :: {#local_file_header{}, string(), integer()}.
-type stream() :: {out(), zlib:zstream(), integer(), list(lh())}.

-spec(open() -> stream()).
open() ->
    {{0, <<>>}, zlib:open(), 0, []}.

-spec write_file(binary(), binary(), stream()) -> stream().
write_file(Filename, Data, {Out, Z, Pos0, Acc}) ->
    FileInfo = get_file_info(Data),
    Size = FileInfo#file_info.size,
    LH = get_file_header(FileInfo, Size, ?STORED, Filename),
    BLH = local_file_header_to_bin(LH),
    Out1 = write_data([<<?LOCAL_FILE_MAGIC:32/little>>, BLH], Out),
    Out2 = write_data(Filename, Out1),
    {Out3, CompSize, CRC} = put_z_file(Out2, Data, 0, Z),
    Patch = <<CRC:32/little, CompSize:32/little>>,
    Out4 = pwrite_data(?LOCAL_FILE_HEADER_CRC32_OFFSET, Patch, Out3),
    Out5 = seek(eof, 0, Out4),
    Pos1 = Pos0 + ?LOCAL_FILE_HEADER_SZ + length(Filename) + CompSize,
    LH2 = LH#local_file_header{comp_size = CompSize, crc32 = CRC},
    Acc2 = [{LH2, Filename, Pos0} | Acc],
    {ok, close_stream(Out5), {{0, <<>>}, Z, Pos1, Acc2}}.

write_file_start(Filename, Size, CRC, {Out, Z, Pos0, Acc}) ->
    FileInfo = get_file_info(Size),
    LH = get_file_header(FileInfo, Size, ?STORED, Filename),
    BLH = local_file_header_to_bin(LH),
    Out1 = write_data([<<?LOCAL_FILE_MAGIC:32/little>>, BLH], Out),
    Out2 = write_data(Filename, Out1),
    {Out3, DPos, CRC1} = put_z_data(Out2, <<>>, 0, Z),
    Patch = <<CRC:32/little, Size:32/little>>,
    Out4 = pwrite_data(?LOCAL_FILE_HEADER_CRC32_OFFSET, Patch, Out3),
    {ok, close_stream(Out4), {{0, <<>>}, Z, Pos0, Acc, {LH, Filename, DPos, CRC1}}}.

write_file_data(Data, {Out, Z, Pos0, Acc, {LH, Filename, DPos0, CRC0}}) ->
    {Out1, DPos, CRC} = put_z_data(Out, Data, DPos0, Z, CRC0),
    {ok, close_stream(Out1), {{0, <<>>}, Z, Pos0, Acc, {LH, Filename, DPos, CRC}}}.

write_file_end({Out, Z, Pos0, Acc, {LH, Filename, CompSize, CRC}}) ->
    Out1 = seek(eof, 0, Out),
    Pos1 = Pos0 + ?LOCAL_FILE_HEADER_SZ + length(Filename) + CompSize,
    LH1 = LH#local_file_header{comp_size = CompSize, crc32 = CRC},
    Acc1 = [{LH1, Filename, Pos0} | Acc],
    {ok, close_stream(Out1), {{0, <<>>}, Z, Pos1, Acc1}}.

close({Out1, Z, Pos, LHS}) ->
    zlib:close(Z),
    Out2 = put_central_dir(lists:reverse(LHS), Pos, Out1, ""),
    {ok, close_stream(Out2)}.

%% ===================================================================
%% Internal functions
%% ===================================================================

get_file_info(Size) when is_integer(Size) ->
    Now = calendar:local_time(),
    #file_info{
        size = Size, type = regular,
        access = read_write, atime = Now,
        mtime = Now, ctime = Now, mode = 0,
        links = 1, major_device = 0,
        minor_device = 0, inode = 0,
        uid = 0, gid = 0};
get_file_info(Data) ->
    get_file_info(byte_size(Data)).

dos_date_time_from_datetime({{Year, Month, Day}, {Hour, Min, Sec}}) ->
    YearFrom1980 = Year-1980,
    <<DosTime:16>> = <<Hour:5, Min:6, Sec:5>>,
    <<DosDate:16>> = <<YearFrom1980:7, Month:4, Day:5>>,
    {DosDate, DosTime}.

get_file_header(#file_info{mtime=MTime}, UncompSize, CompMethod, Name) ->
    {ModDate, ModTime} = dos_date_time_from_datetime(MTime),
    #local_file_header{
        version_needed = 20,
        gp_flag = 0,
        comp_method = CompMethod,
        last_mod_time = ModTime,
        last_mod_date = ModDate,
        crc32 = -1,
        comp_size = -1,
        uncomp_size = UncompSize,
        file_name_length = length(Name),
        extra_field_length = 0}.

local_file_header_to_bin(
    #local_file_header{
        version_needed = VersionNeeded,
        gp_flag = GPFlag,
        comp_method = CompMethod,
        last_mod_time = LastModTime,
        last_mod_date = LastModDate,
        crc32 = CRC32,
        comp_size = CompSize,
        uncomp_size = UncompSize,
        file_name_length = FileNameLength,
        extra_field_length = ExtraFieldLength}) ->
    <<VersionNeeded:16/little,
    GPFlag:16/little,
    CompMethod:16/little,
    LastModTime:16/little,
    LastModDate:16/little,
    CRC32:32/little,
    CompSize:32/little,
    UncompSize:32/little,
    FileNameLength:16/little,
    ExtraFieldLength:16/little>>.

eocd_to_bin(#eocd{
        disk_num = DiskNum,
        start_disk_num = StartDiskNum,
        entries_on_disk = EntriesOnDisk,
        entries = Entries,
        size = Size,
        offset = Offset,
        zip_comment_length = ZipCommentLength}) ->
    <<DiskNum:16/little,
    StartDiskNum:16/little,
    EntriesOnDisk:16/little,
    Entries:16/little,
    Size:32/little,
    Offset:32/little,
    ZipCommentLength:16/little>>.

cd_file_header_to_bin(
    #cd_file_header{version_made_by = VersionMadeBy,
        version_needed = VersionNeeded,
        gp_flag = GPFlag,
        comp_method = CompMethod,
        last_mod_time = LastModTime,
        last_mod_date = LastModDate,
        crc32 = CRC32,
        comp_size = CompSize,
        uncomp_size = UncompSize,
        file_name_length = FileNameLength,
        extra_field_length = ExtraFieldLength,
        file_comment_length = FileCommentLength,
        disk_num_start = DiskNumStart,
        internal_attr = InternalAttr,
        external_attr = ExternalAttr,
        local_header_offset = LocalHeaderOffset}) ->
    <<VersionMadeBy:16/little,
    VersionNeeded:16/little,
    GPFlag:16/little,
    CompMethod:16/little,
    LastModTime:16/little,
    LastModDate:16/little,
    CRC32:32/little,
    CompSize:32/little,
    UncompSize:32/little,
    FileNameLength:16/little,
    ExtraFieldLength:16/little,
    FileCommentLength:16/little,
    DiskNumStart:16/little,
    InternalAttr:16/little,
    ExternalAttr:32/little,
    LocalHeaderOffset:32/little>>.

%% zip header functions
cd_file_header_from_lh_and_pos(LH, Pos) ->
    #local_file_header{
        version_needed = VersionNeeded,
        gp_flag = GPFlag,
        comp_method = CompMethod,
        last_mod_time = LastModTime,
        last_mod_date = LastModDate,
        crc32 = CRC32,
        comp_size = CompSize,
        uncomp_size = UncompSize,
        file_name_length = FileNameLength,
        extra_field_length = ExtraFieldLength} = LH,
    #cd_file_header{
        version_made_by = 20,
        version_needed = VersionNeeded,
        gp_flag = GPFlag,
        comp_method = CompMethod,
        last_mod_time = LastModTime,
        last_mod_date = LastModDate,
        crc32 = CRC32,
        comp_size = CompSize,
        uncomp_size = UncompSize,
        file_name_length = FileNameLength,
        extra_field_length = ExtraFieldLength,
        file_comment_length = 0, % FileCommentLength,
        disk_num_start = 1, % DiskNumStart,
        internal_attr = 0, % InternalAttr,
        external_attr = 0, % ExternalAttr,
        local_header_offset = Pos}.

write_data(Data, {Pos, B}) ->
    {Pos + erlang:iolist_size(Data), pwrite_binary(B, Pos, Data)}.

pwrite_data(Pos, Data, {OldPos, B}) ->
    {OldPos, pwrite_binary(B, Pos, Data)}.

close_stream({_, B}) ->
    B.

pwrite_binary(B, Pos, Bin) ->
    erlang:iolist_to_binary(pwrite_iolist(B, Pos, Bin)).

seek(cur, Pos, {OldPos, B}) ->
    {OldPos + Pos, B};
seek(eof, Pos, {_, B}) ->
    {byte_size(B) + Pos, B}.

%% A pwrite-like function for iolists (used by memory-option)

split_iolist(B, Pos) when is_binary(B) ->
    split_binary(B, Pos);
split_iolist(L, Pos) when is_list(L) ->
    splitter([], L, Pos).

splitter(Left, Right, 0) ->
    {Left, Right};
splitter(Left, [A | Right], RelPos) when is_list(A) or is_binary(A) ->
    Sz = erlang:iolist_size(A),
    case Sz > RelPos of
        true ->
            {Leftx, Rightx} = split_iolist(A, RelPos),
            {[Left | Leftx], [Rightx, Right]};
        _ ->
            splitter([Left | A], Right, RelPos - Sz)
    end;
splitter(Left, [A | Right], RelPos) when is_integer(A) ->
    splitter([Left, A], Right, RelPos - 1);
splitter(Left, Right, RelPos) when is_binary(Right) ->
    splitter(Left, [Right], RelPos).

skip_iolist(B, Pos) when is_binary(B) ->
    case B of
        <<_:Pos/binary, Bin/binary>> -> Bin;
        _ -> <<>>
    end;
skip_iolist(L, Pos) when is_list(L) ->
    skipper(L, Pos).

skipper(Right, 0) ->
    Right;
skipper([A | Right], RelPos) when is_list(A) or is_binary(A) ->
    Sz = erlang:iolist_size(A),
    case Sz > RelPos of
        true ->
            Rightx = skip_iolist(A, RelPos),
            [Rightx, Right];
        _ ->
            skip_iolist(Right, RelPos - Sz)
    end;
skipper([A | Right], RelPos) when is_integer(A) ->
    skip_iolist(Right, RelPos - 1).

pwrite_iolist(Iolist, Pos, Bin) ->
    {Left, Right} = split_iolist(Iolist, Pos),
    Sz = erlang:iolist_size(Bin),
    R = skip_iolist(Right, Sz),
    [Left, Bin | R].

put_z_file(Out0, Data, Pos0, Z) ->
    put_z_data(Out0, Data, Pos0, Z).
    %CRC0 = zlib:crc32(Z, <<>>),
    %Out1 = write_data(Data, Out0),
    %CRC = zlib:crc32(Z, CRC0, Data),
    %{Out1, Pos0 + erlang:iolist_size(Data), CRC}.

put_z_data(Out, Data, Pos0, Z) ->
    put_z_data(Out, Data, Pos0, Z, zlib:crc32(Z, <<>>)).

put_z_data(Out0, Data, Pos0, Z, CRC0) ->
    Out1 = write_data(Data, Out0),
    CRC = zlib:crc32(Z, CRC0, Data),
    {Out1, Pos0 + iolist_size(Data), CRC}.

%% put the central directory, at the end of the zip archive
put_central_dir(LHS, Pos, Out0, Comment) ->
    {Out1, Sz} = put_cd_files_loop(LHS, Out0, 0),
    put_eocd(length(LHS), Pos, Sz, Comment, Out1).

put_cd_files_loop([], Out, Sz) ->
    {Out, Sz};
put_cd_files_loop([{LH, Name, Pos} | LHRest], Out0, Sz0) ->
    CDFH = cd_file_header_from_lh_and_pos(LH, Pos),
    BCDFH = cd_file_header_to_bin(CDFH),
    B = [<<?CENTRAL_FILE_MAGIC:32/little>>, BCDFH, Name],
    Out1 = write_data(B, Out0),
    Sz1 = Sz0 + ?CENTRAL_FILE_HEADER_SZ + LH#local_file_header.file_name_length,
    put_cd_files_loop(LHRest, Out1, Sz1).

%% put end marker of central directory, the last record in the archive
put_eocd(N, Pos, Sz, Comment, Out0) ->
    %% BComment = list_to_binary(Comment),
    CommentSz = length(Comment), % size(BComment),
    EOCD = #eocd{
        disk_num = 0,
        start_disk_num = 0,
        entries_on_disk = N,
        entries = N,
        size = Sz,
        offset = Pos,
        zip_comment_length = CommentSz},
    BEOCD = eocd_to_bin(EOCD),
    B = [<<?END_OF_CENTRAL_DIR_MAGIC:32/little>>, BEOCD, Comment], % BComment],
    write_data(B, Out0).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

zip_test_() ->
    {setup, 
        fun() -> ok end, 
        fun(_) -> os:cmd("rm -rf archive.zip folder*") end,
        [
            {"zipstream file", fun zipstream_file/0},
            {"zipstream chunk", fun zipstream_chunk/0}
        ]
    }.

digest2str(Digest) -> bin2str(binary_to_list(Digest)).

bin2str([H | T]) ->
    {H1, H2} = byte2hex(H),
    [H1, H2 | bin2str(T)];
bin2str([]) ->
    [].

byte2hex(X) -> 
    {nibble2hex(X bsr 4), nibble2hex(X band 15)}.

nibble2hex(X) when X >= 0, X =< 9 -> X + $0;
nibble2hex(X) when X >= 10, X =< 15 -> X - 10 + $a.

data_hash(Bin) ->
    digest2str(crypto:sha(Bin)).

file_hash(File) ->
    {ok, Bin} = file:read_file(File),
    data_hash(Bin).

zipstream_file() ->
    S = open(),
    {P1, D1} = {"folder1/file1", <<"data file1">>},
    {ok, B1, S1} = write_file(P1, D1, S),
    {P2, D2} = {"folder2/file2", <<"data file2">>},
    {ok, B2, S2} = write_file(P2, D2, S1),
    {ok, B3} = close(S2),
    B = iolist_to_binary([B1,B2,B3]),
    ok = file:write_file("archive.zip", B),
    os:cmd("unzip archive.zip"),
    true = filelib:is_dir("folder1"),
    true = filelib:is_regular("folder1/file1"),
    true = filelib:is_dir("folder2"),
    true = filelib:is_regular("folder2/file2"),
    {ok, D1} = file:read_file(P1),
    Hash1 = file_hash("folder1/file1"),
    Hash1 = data_hash(D1),
    {ok, D2} = file:read_file(P2),
    Hash2 = file_hash("folder2/file2"),
    Hash2 = data_hash(D2).

zipstream_chunk() ->
    S = open(),
    {F1, Chunks1} = {"folder1/file1", [<<"data ">>, <<"file1">>]},
    {F2, Chunks2} = {"folder2/file2", [<<"data ">>, <<"file2">>]},
    GetCRC = fun(Chunks) ->
            Z = zlib:open(),
            lists:foldl(fun(C, A) -> 
                        zlib:crc32(Z, A, C) 
                end, zlib:crc32(Z, <<>>), Chunks)
    end,
    C = fun(N, Chk) -> lists:nth(N, Chk) end,
    {ok, B1, S1} = write_file_start(F1, iolist_size(Chunks1), GetCRC(Chunks1), S),
    {ok, B2, S2} = write_file_data(C(1, Chunks1), S1),
    {ok, B3, S3} = write_file_data(C(2, Chunks1), S2),
    {ok, B4, S4} = write_file_end(S3),
    {ok, B5, S5} = write_file_start(F2, iolist_size(Chunks2), GetCRC(Chunks2), S4),
    {ok, B6, S6} = write_file_data(C(1, Chunks2), S5),
    {ok, B7, S7} = write_file_data(C(2, Chunks2), S6),
    {ok, B8, S8} = write_file_end(S7),
    {ok, B9} = close(S8),
    B = iolist_to_binary([B1,B2,B3,B4,B5,B6,B7,B8,B9]),
    ok = file:write_file("archive.zip", B),
    os:cmd("unzip archive.zip"),
    true = filelib:is_dir("folder1"),
    true = filelib:is_dir("folder2"),
    true = filelib:is_regular(F1),
    true = filelib:is_regular(F2),
    Hash1 = file_hash(F1),
    Hash1 = data_hash(<<"data file1">>),
    Hash2 = file_hash(F2),
    Hash2 = data_hash(<<"data file2">>).

-endif.
