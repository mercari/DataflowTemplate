#.1 Writing variables directly

SingerId,AlbumId,TrackId,Singer,AlbumTitle,SongName
<#list Songs?sort_by('AlbumId') as input>
${SingerId},${input.AlbumId},${input.TrackId},${input.FirstName} ${input.LastName},${input.AlbumTitle},${input.SongName}
</#list>


#.2 Use the built-in class _CSVPrinter to safely write one line at a time.
(Each field can be processed in detail)

SingerId,AlbumId,TrackId,SongName,SongGenre,AlbumTitle,Name
<#list Songs?sort_by('AlbumId') as input>
${_CSVPrinter.line(SingerId, input.AlbumId, input.TrackId, input.SongName, input.SongGenre, input.AlbumTitle, input.FirstName + " " + input.LastName)}
</#list>


#.3 Use the built-in class _CSVPrinter to group array values together and describe them safely.
(Detailed customization for each field is not possible, but it is a simple description)

SingerId,AlbumId,TrackId,SongName,SongGenre,AlbumTitle,FirstName
${_CSVPrinter.lines(Songs?sort_by('AlbumId'), "SingerId", "AlbumId", "TrackId", "SongName", "SongGenre", "AlbumTitle", "FirstName")}
