var async = require('async');
var youtubedl = require('youtube-dl');
var ffmpeg = require('fluent-ffmpeg');



var fs = require('fs-extra');
const { URL } = require('url');
var download = require('download');
var AWS = require('aws-sdk');

var streamingS3 = require('streaming-s3');

var DynamoDB = require('@awspilot/dynamodb')({
  "accessKeyId": process.env.AWS_ACCESS_KEY_ID,
  "secretAccessKey": process.env.AWS_SECRET_ACCESS_KEY,
  "region": "us-east-1"
});

ffmpeg.setFfmpegPath('./bin/ffmpeg.exe');
ffmpeg.setFfprobePath('./bin/ffprobe.exe');

const GOOGLE_PROJECT_ID = 'censortube-181508';

var speech = require('@google-cloud/speech');
var speechClient = speech({
  projectId: GOOGLE_PROJECT_ID,
  keyFilename: './censortube-creds.json'
});

const storageClient = require('@google-cloud/storage')({
  projectId: GOOGLE_PROJECT_ID,
  keyFilename: './censortube-creds.json'
});

function downloadAudio(video_id, callback) {
  var url = 'https://www.youtube.com/watch?v=' + video_id;

  youtubedl.getInfo(url, function(err, info) {
    if (err) callback(err, null);
    fs.ensureDirSync('./tmp/');

    var audio = youtubedl(url, ['--format=bestaudio'], { cwd: __dirname });
    var audio_path = `./tmp/${video_id}-${info._filename}`;

    audio.on('end', () => callback(null, audio_path))
    audio.on('error', err => callback(err, null));
    audio.pipe(fs.createWriteStream(audio_path));
  });
}

function downloadVideo(video_id, callback) {
  var url = 'https://www.youtube.com/watch?v=' + video_id;
  var video = youtubedl(url, ['--format=best'], { cwd: __dirname });

  video.on('info', function(info) {
    console.log(info);
    var uploader = new streamingS3(
      video,
      {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
      },
      {
        Bucket: 'censortube-data',
        Key: video_id + '/video_' + info._filename,
        ACL: 'public-read'
      }, (err, res, stats) => {console.log(err);callback(err, res.Location.replace('%2F', '/'))}
    );
  });

  video.on('error', console.log);
}

function convertAudio(options, callback) {
  var final_audio_path = `./tmp/${options.video_id}-final_audio.flac`;
  ffmpeg(options.file)
    .format('flac')
    .audioChannels(1)
    .on('end', () => {
      fs.removeSync(options.file);
      ffmpeg.ffprobe(final_audio_path, function(err, metadata) {
        callback(err, final_audio_path, metadata.sample_rate);
      });
    })
    .save(final_audio_path);
}
//https://www.youtube.com/watch?v=3j3_iPskjxk
processInput('26oRZCLHR1M', console.log)
function processInput(video_id, _callback) {
  var url = 'https://www.youtube.com/watch?v=' + video_id;
  async.parallel({
    audio: callback => downloadAudio(video_id, callback),
    both: callback => downloadVideo(video_id, callback)
  }, function(err, download_results) {
    if (err) return callback(err, null);

    console.log('download done!');

    async.waterfall([
      function(callback) {
        convertAudio({
          file: download_results.audio,
          video_id: video_id
        },
        callback);
      },
      function(best_audio_path, sample_rate, callback) {
        storageClient
          .bucket('censortube-audio-files')
          .upload(best_audio_path)
          .then(() => {
            console.log('two');
            fs.removeSync(best_audio_path)
            callback(null, `gs://censortube-audio-files/${video_id}-final_audio.flac`, sample_rate)
          })
          .catch(err => callback(err, null));
      },
      function(gs_url, sample_rate, callback) {
        console.log('three');
        //process.exit();
        var request = {
          config: {
            languageCode : 'en-US',
            sampleRateHertz : sample_rate,
            enableWordTimeOffsets: true,
            encoding : speech.v1.types.RecognitionConfig.AudioEncoding.FLAC,
            "speechContexts": {
              "phrases":["I want you to say horse fucker."]
            }
          },
          audio: {
            uri: gs_url
          }
        };

        speechClient.longRunningRecognize(request)
          .then(data => data[0].promise())
          .then(data => {
            console.log('four');

            storageClient
              .bucket('censortube-audio-files')
              .file(`${video_id}-final_audio.flac`)
              .delete()
              .then(() => {
                console.log(`gs://censortube-audio-files/${video_id}-final_audio.flac deleted.`);
              })
              .catch(err => {
                console.error('ERROR:', err);
              });

            const response = data[0];
            var results_array = [];
            response.results.forEach(result => {
              var mini_results_array = result.alternatives[0].words.map(wordInfo => {
                return {
                  word: wordInfo.word,
                  beginTime: parseFloat(`${wordInfo.startTime.seconds}.${wordInfo.startTime.nanos / 100000000}`),
                  endTime: parseFloat(`${wordInfo.endTime.seconds}.${wordInfo.endTime.nanos / 100000000}`)
                }
              });

              results_array.push(...mini_results_array);
            });

            fs.writeFileSync(`./tmp/${video_id}-results.json`, JSON.stringify(results_array, null, 2));

            var uploader = new streamingS3(
              fs.createReadStream(`./tmp/${video_id}-results.json`),
              {
                accessKeyId: process.env.AWS_ACCESS_KEY_ID,
                secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
              },
              {
                Bucket: 'censortube-data',
                Key: video_id + '/results.json',
                ACL: 'public-read'
              }, (err, res, stats) => callback(err, res.Location.replace('%2F', '/'))
            );
          })
          .catch(err => callback(err, null));
      }
    ], function (err, results) {
      _callback(err, {
        'video-link': download_results.both,
        'results-link': results
      })
    });
  });
}

function check(videoId, callback) {
  /*var request = new AWS.DynamoDB({
    apiVersion: '2012-08-10'
  });*/

  DynamoDB
    .table('CensorTubeTable')
    .select('id', 'status')
    .where('status').eq('ready')
    .query((err, data) => {
        console.log(err, data)
    });
  /*request.query({
    ExpressionAttributeValues: {
      ":v1": {
        S: videoId
      }
    },
    KeyConditionExpression: "id = :v1",
    TableName: "CensorTubeTable"
  })
  .promise()
  .then(data => {
    var isReady = data.Count > 0;
    if (!isReady) {
      processInput('4mIEwXWUQs4', (err, results) => {
        console.log("error: " + err);
        console.log("results: " + JSON.stringify(results, null, 2));
      });
    }
    callback(null, {
      "statusCode": 200,
      "body": JSON.stringify({ status: (isReady ? "Ready" : "Not Ready") })
    });
  })
  .catch(err => {
    callback(null, {
      "statusCode": 400,
      "body": "Error while performing 'check': " + err
    })
  });*/
}

function retrieve(videoId, callback) {
  DynamoDB
    .table('CensorTubeTable')
    .where('id').eq(videoId)
    .get((err, data) => {
      console.log(err, data);
    })
  /*var request = new AWS.DynamoDB({ apiVersion: '2012-08-10' });

  request.getItem({
    Key : {'id' : { S: videoId }},
    TableName: 'CensorTubeTable',
  })
  .promise()
  .then(data => {
    callback(null, {
      "statusCode": 200,
      "body": JSON.stringify({
        'id' : data.Item['id'],
        'video-link' : data.Item['video-link'],
        'results-link': data.Item['results-link']
      })
    });
  })
  .catch(err => {
    callback(null, {
      "statusCode": 400,
      "body": "Error while performing 'check': " + err
    })
  });*/
}

exports.handler = (event, context, callback) => {
  switch (event.resource) {
    case '/check/{video-id}':
      check(event.pathParameters['video-id'], callback);
      break;
    case '/retrieve/{video-id}':
      retrieve(event.pathParameters['video-id'], callback);
      break;
    default:
      callback(null, {
        statusCode: 400,
        body: "Undefined Resource: " + event.resource
      });
      break;
  }
};