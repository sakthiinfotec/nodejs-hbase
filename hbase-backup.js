/* 
 * node hbase-backup.js
 * Pre requisites: npm install hbase@0.1.1
 */

var fs = require('fs');
var hbase = require('hbase');

var startRow = '20130515';
var endRow = '20130814';
var format = 'csv'; // csv | tsv
var separator = (format === 'csv')?',':'\t';
var htable = 'bigtable';
var bkup_file = htable+'-'+ new Date().getTime()+'.'+format;
var showCount = 100000; 
  
var options = {batch:10000, startRow:startRow, endRow:endRow};
var data = null;
hbase({ host: 'ec2-23-20-198-200.compute-1.amazonaws.com', port: 8080 })
    .getScanner('bigtable')
    .create(options, function(err, scannerId) {
        if (err) {
            console.log('Scanner creation error: ' + err);
        } else {
            var count = 0;
            console.log('Scanner '+scannerId+' created ...');
            this.get(function(error, cells){
                cells && cells.forEach(function(cell) {
                    data = cell.key+separator+cell.column+separator+cell.timestamp+separator+cell['$']+'\n';
                    fs.appendFileSync(bkup_file, data);
                    if(count > 0 && count % showCount == 0) {
                        console.log('Completed '+count+' cells.')
                    }
                    count++;
                });
                
                if(cells) { // call the next iteration
                  console.log('Completed '+count+' cells.')
                  this.continue();
                } else {
                  console.log('Backup completed.');
                  return this.delete();
                }
            });
        }
    });
