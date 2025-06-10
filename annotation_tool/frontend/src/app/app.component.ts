import {Component, Injectable, OnInit} from '@angular/core';
import {TableRowDTO} from "./model/tableRowDTO";
import {APIS, CSVTableRowAPIService} from "./api/api";
import {HttpClient} from "@angular/common/http";
import {error} from "@angular/compiler-cli/src/transformers/util";
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import { MatSnackBar } from '@angular/material';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
  providers: [
    CSVTableRowAPIService ]
})
@Injectable()
export class AppComponent {

  title = 'UIAnotator';
  tableRows!: TableRowDTO[];
  Object!: ObjectConstructor;
  isLoaded: Boolean = false;


  constructor(private http: HttpClient, private csvTableRowAPIService: CSVTableRowAPIService, private snackBar: MatSnackBar) {
    this.Object = Object;
    csvTableRowAPIService.tablesRowsGet5UnansweredRowsGet().forEach(
      rows => this.tableRows = rows
    ).then(r => {
        this.isLoaded = true;
        r;
    });
  }

  submitAll() {
    if (!this.validate()) {
      this.snackBar.open("Fehlende Felder, ueberpruefen Sie Ihre Eingabe!", "Verstanden.");
      return false;
    } else {
      let observer = this.csvTableRowAPIService.tablesRowsSubmit5UnansweredRowsPost(this.tableRows)
        .subscribe({
          complete: () => {
            this.snackBar.open("Sie sind fertig. Die Seite laedt sich in 2s neu...", "Verstanden.", {
              duration: 2500,
            })
            setTimeout(function(){
              window.location.reload();
              location.reload();
            },2000);
          }
        });

      return true;
    }
  }


  validate(): boolean{
    for(let row of this.tableRows){

      console.log(row.question + ":" + row.answerColumn)

      if(row.question == "" || row.question == null){
        return false;
      }

      if(row.answerColumn == null){
        return false;
      }

    }
    return true;
  }


}
