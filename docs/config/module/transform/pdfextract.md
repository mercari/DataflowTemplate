# PDF extract Transform Module

Transform module for extract text from pdf content.
(You can also specify the file storage path where the PDF file is placed for extraction)

## Transform module common parameters

| parameter  | optional | type                | description                                                                                                 |
|------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                                                           |
| module     | required | String              | Specified `pdfextract`                                                                                      |
| inputs     | required | Array<String\>      | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                                |

## PDF extract transform module parameters

| parameter | optional | type   | description                                                                                                      |
|-----------|----------|--------|------------------------------------------------------------------------------------------------------------------|
| field     | required | String | A field name whose content is a byte array of the PDF file or the path of the GCS where the PDF file is located. |
| prefix    | optional | String | Specify this option if you want to add a prefix to the following field names extracted from PDF files.           |


## Fields to be extracted from the PDF file

This module extracts the following information from a pdf file by using [Apache PDFBox](https://pdfbox.apache.org/).

| parameter        | type      | description                                                                    |
|------------------|-----------|--------------------------------------------------------------------------------|
| Content          | String    | Text extracted from PDF file.                                                  |
| FileByteSize     | Integer   | Byte size of PDF file.                                                         |
| Page             | Integer   | Number of pages in the PDF file.                                               |
| Version          | String    | PDF file version                                                               |
| Encrypted        | Boolean   | PDF file is  encrypted or not.                                                 |
| Title            | String    | Title of the document.                                                         |
| Author           | String    | Name of the person who created the document.                                   |
| Subject          | String    | Subject of the document.                                                       |
| Keywords         | String    | Keywords for documents.                                                        |
| Creator          | String    | The original creation tool if converted from a format other than PDF.          |
| Producer         | String    | The conversion tool if converted from a format other than PDF.                 |
| CreationDate     | Timestamp | The datetime when the document was generated.                                  |
| ModificationDate | Timestamp | The last datetime the document was updated.                                    |
| Trapped          | String    | Whether or not the document has been modified to include trapping information. |
| Failed           | Boolean   | True if PDF file parsing fails.                                                |
| ErrorPageCount   | Integer   | Number of pages of failed to parse PDFs.                                       |
| ErrorMessage     | String    | Error message if errors occur in PDF parsing.                                  |


## Related example config files

* [BigQuery to Solr Index(with text from PDF)](../../../../examples/bigquery-pdf-to-solrindex.json)
