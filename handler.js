'use strict';
var AWS = require('aws-sdk');
var fs = require("fs");
var path = require("path");
var exec = require('child_process').exec;
var csv = require("fast-csv");
var s3 = new AWS.S3();
const S3_IMAGE_BUCKET = 'digitalcollection';
const S3_IMAGE_CSV_BUCKET = 'images-csv';
var params = { Bucket: S3_IMAGE_BUCKET }
var exif_cmd = '  ';
var xmpoutput = '';
var tmpdir = '/tmp/';
var csvfilename = 'Images.csv';
var outputcsvpath = tmpdir + csvfilename;
var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });  //change to your region
var lambda = new AWS.Lambda({
    region: 'us-east-1' //change to your region
});
var batch = new AWS.Batch({ apiVersion: '2016-08-10', region: 'us-east-1' });
//Main Function    
var objectfile = '';
var inputfile = '';
exports.Aggregator = (event, context, callback) => {
    console.log("----- lamda triggered after s3 objected created----------");    
    var strevent = JSON.stringify(event);
    process.env['PATH'] = process.env['PATH'] + ':' + process.env['LAMBDA_TASK_ROOT'];
    var fileSize = event.Records[0].s3.object.size
    if (process.env["IsJob"] == 'true') {
        var params = {
            jobDefinition:"s3jp2convrter-def:2",  // Job defenition name
            jobName:"s3jp2convrter-job",          // Job name
            jobQueue:"s3jp2convrter-queue",       // Jo queue name
            containerOverrides: { command: [strevent] }  
        };
        batch.submitJob(params, function (err, data) {
            if (err) { console.log(err, err.stack); } // an error occurred
            else { console.log("batch job data", data); }
        });
    }
    objectfile = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " ")); // Uploaded file name
    inputfile = objectfile.replace(/\ /g, '_');
    inputfile = tmpdir + inputfile.replace(/\//g, '_');  
    //Get the extension
    var srcFileExt = inputfile.substr(inputfile.lastIndexOf('.') + 1);
    var srcFileKey = inputfile.substr(0, inputfile.lastIndexOf('.'));
    //Writing CSV File
    var inputfile1 = srcFileKey.replace(/\ /g, '_');
    inputfile1 = inputfile1.replace(/\//g, '_');
    outputcsvpath = tmpdir + inputfile1 + '_' + csvfilename;
    //Check the Validation and return if other than image file
    var validImageTypes = ['PNG','png', 'tif', 'tiff', 'TIF', 'TIFF','jpg','jpeg','JPG','JPEG','gif','GIF'];
    if (validImageTypes.indexOf(srcFileExt) < 0) {
        callback(null, {
            status: false,
            message: 'Image extension does not match.'
        });
    }  
    exec('rm -rf ' + tmpdir + "*", function (error, stdout, stderr) {
        //Get Object for the Image
        var stream = s3.getObject({
            Bucket: S3_IMAGE_BUCKET,
            Key: objectfile
        }).createReadStream();
        stream.on('end', function () {
            //Process the image to extract the meta data           
            xmpoutput = srcFileKey + '.json'
            exif_cmd = "perl /var/task/exiftool_lib/exiftool  -api XMPAutoConv=0 -json -b " + inputfile + " > " + xmpoutput;                    
            exifrun(exif_cmd, xmpoutput, function (error) {
                if (error) {
                    exec('rm -rf ' + tmpdir + "*", function (error, stdout, stderr) { });
                    console.error(`exec error- remove temp:`, error);
                    callback(error);
                }
                else {
                    exec('rm -rf ' + tmpdir + "*", function (error, stdout, stderr) { });                   
                    callback(null);
                }
            });
        });
        stream.on('error', function (err) {
            console.log("download error", err);
            exec('rm -rf ' + tmpdir + "*", function (error, stdout, stderr) {});
            callback(err);
        });
        var file = fs.createWriteStream(inputfile);
        stream.pipe(file);
    });
}

function exifrun(exif_cmd, xmpoutput, callback) {  
    exec(exif_cmd, function (error, stdout, stderr) {        
        if (error) {
            console.error(`exec error when metdata extract:`, error);
            callback(error);
        }
        else {
            var obj = require(xmpoutput);
            obj = obj[0];           
            ManageImageMetadata(obj, function (error) {
                if (error) {
                    callback(error);
                } else {
                    callback(null)
                };
            });
        }ss
    });
}
function ManageImageMetadata(metadata, callback) {
    console.log("----------ManageImageMetadata------- metadata:", metadata);
    var srcFileExt = objectfile.substr(objectfile.lastIndexOf('.') + 1);
    var srcFileKey = objectfile.substr(0, objectfile.lastIndexOf('.'));
    var primarydisplay = metadata.AGOSelectedview;
    if (metadata.AGOSelectedview != undefined) {
        if (metadata.AGOSelectedview.toLowerCase() == 'yes') primarydisplay = "1";
        if (metadata.AGOSelectedview.toLowerCase() == 'no') primarydisplay = "0";
    }
    var status = metadata.AGOStatus.toLowerCase();
    if (status == 'complete') status = "INSERT";
    var dbparams = {
        TableName: "digitalcollection",
        Item: {
            ObjectID: metadata.AGOObjectID.toString(),
            OriginalObjectNumber: metadata.AGOACCno.toString(),
            FileName: objectfile.toLowerCase(),
            PixelH: metadata.ImageHeight,
            PixelW: metadata.ImageWidth,
            ImageName: objectfile.toLowerCase().replace(srcFileExt, 'jp2'),
            PrimaryDisplay: primarydisplay,
            STATUS: status,
            FileKey: objectfile,
            CsvFileKey: srcFileKey + '_' + csvfilename
        }
    }   
    docClient.put(dbparams, function (err, data) {
        if (err) {
            console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
            callback(err);
        } else {            
            callback(null);
        }
    });
}

