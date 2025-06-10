# Table Annotator Backend

In order to run backend it is necessary to establish a datasource and provide application.properties file with necessary data for connecting with your datasource 

## Running the application in dev mode

You can run the application in dev mode that enables live coding using:
```shell script
  ./mvnw compile quarkus:dev
```
If you want to initialize the tables specified data.csv, run the application with 
```shell script
./mvnw package
```  
and then ``` java -jar target/quarkus-app/quarkus-run.jar --enable-table-init```.

If you want to export the database and so far collected data, start the application with 
```shell script
./mvnw package
``` 
and then ``` java -jar target/quarkus-app/quarkus-run.jar --enable-export-tables```.

## Path Table

| Method | Path | Description |
| --- | --- | --- |
| GET | [/tables/rows/get5UnansweredRows](#gettablesrowsget5unansweredrows) |  |
| POST | [/tables/rows/submit5UnansweredRows](#posttablesrowssubmit5unansweredrows) |  |

## Reference Table

| Name | Path | Description |
| --- | --- | --- |
| TableCellDTO | [#/components/schemas/TableCellDTO](#componentsschemastablecelldto) |  |
| TableRowDTO | [#/components/schemas/TableRowDTO](#componentsschemastablerowdto) |  |

## Path Details

***

### [GET]/tables/rows/get5UnansweredRows

#### Responses

- 200 OK

`application/json`

```ts
{
  tableId?: string
  rowEntries: {
    cellValue?: string
    rowName?: string
    columnId?: integer
  }[]
  question?: string
  row?: integer
  answerColumn?: integer
  content?: string
}[]
```

***

### [POST]/tables/rows/submit5UnansweredRows

#### RequestBody

- application/json

```ts
{
  tableId?: string
  rowEntries: {
    cellValue?: string
    rowName?: string
    columnId?: integer
  }[]
  question?: string
  row?: integer
  answerColumn?: integer
  content?: string
}[]
```

#### Responses

- 201 Created

## References

### #/components/schemas/TableCellDTO

```ts
{
  cellValue?: string
  rowName?: string
  columnId?: integer
}
```

### #/components/schemas/TableRowDTO

```ts
{
  tableId?: string
  rowEntries: {
    cellValue?: string
    rowName?: string
    columnId?: integer
  }[]
  question?: string
  row?: integer
  answerColumn?: integer
  content?: string
}
```
