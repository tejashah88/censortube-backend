var async = require('async');

var fs = require('fs-extra');
const { URL } = require('url');
var download = require('download');
var AWS = require('aws-sdk');

const ytdl = require('ytdl-core');

var request = require('request');

const TransloaditClient = require('transloadit')
const transloadit = new TransloaditClient({
  authKey: process.env.TRANSLOADIT_KEY
  authSecret: process.env.TRANSLOADIT_SECRET
});

function getFlacFile(video_id, callback) {
  var url = 'https://www.youtube.com/watch?v=' + video_id;

  ytdl.getInfo(url, { filter: 'audio' }, function(err, info) {
    var audio_info = info.formats.filter(x => !!x.type && x.type.includes('webm'))[0];
    console.log(audio_info);
    download(audio_info.url).then(data => {
      fs.writeFileSync('./tmp/' + video_id + '-audio.' + audio_info.container, data);
      return;
    })
    .then(() => {
      const fieldName = video_id;
      const filePath = './tmp/' + video_id + '-audio.' + audio_info.container;

      transloadit.addFile(fieldName, filePath)
      transloadit.createAssembly({
        params: {
          steps: {
            encode: {
              use: ":original",
              robot: "/audio/encode",
              result: true,
              preset: "flac",
              //sample_rate: 44100,
              audio_channels: 1,
              ffmpeg_stack: "v2.2.3"
            }
          }
        }
      }, function(err, result) {
        if (err) {
          throw new Error(err);
        }

        var resp = null;

        async.until(function() {
          return !!resp;
        }, function(callback) {
          transloadit.getAssembly(result.assembly_id, (err, results) => {
            if (results.message == 'The assembly was successfully completed.') resp = true;
            callback(err, results.results);
          })
        }, (err, final) => {
          console.log(final)
          callback(err, final.encode[0].url);
        })
      });
    })
    .catch(console.log);
  });
}

var s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});

const storageClient = require('@google-cloud/storage')({
  projectId: process.env.GOOGLE_PROJECT_ID,
  keyFilename: './censortube-creds.json'
});

//getVideoUrl('4mIEwXWUQs4');
//processVideo('4mIEwXWUQs4', console.log)
recognizeSpeech(`gs://censortube-audio-files/4mIEwXWUQs4-audio-final.flac`, 44100, '4mIEwXWUQs4', 1000, console.log)
function getVideoUrl(video_id, callback) {
  var url = 'https://www.youtube.com/watch?v=' + video_id;

  ytdl.getInfo(url, function(err, info) {
    if (err) callback(err, null);
    var video_url = info.formats[0].url;
    console.log(err, video_url);
    updateVideoLink(video_id, video_url, callback);
  });
}

function recognizeSpeech(uri, sample_rate, video_id, delay, final_callback) {
  request.post('https://speech.googleapis.com/v1/speech:longrunningrecognize',
  {
    headers: {
      'Authorization': 'Bearer ' + process.env.SPEECH_BEARER_TOKEN
    },
    json: {
      config: {
        languageCode : 'en-US',
        sampleRateHertz : sample_rate,
        enableWordTimeOffsets: true,
        encoding : 'FLAC'
      },
      audio: {
        uri: uri
      }
    }
  },
  function (error, response, body) {
    console.log('uri: ' + uri)
    console.log('error:', error);
    console.log('body:', body);

    var resp = null;

    async.until(function() {
      return !!resp;
    }, function(callback) {
      request.get(
        `https://speech.googleapis.com/v1/operations/${body.name}`,{
          headers: {
            'Authorization': 'Bearer ' + bearerToken
          }
        },
        function (_error, response, _body) {
          var final_body = JSON.parse(_body);
          resp = !!final_body.done;
          setTimeout(() => {
            callback(null, final_body.response);
          }, delay)
        }
      );
    }, (err, final) => {
      var results_array = [];
      final.results.forEach(result => {
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

      var params = {
        Body: fs.readFileSync(`./tmp/${video_id}-results.json`),
        Bucket: "censortube-data",
        Key: video_id + '/results.json',
        ACL: 'public-read'
      };
      s3.putObject(params, (err, data) => {
        if (err) throw err;
        updateResultsLink(
          video_id,
          `https://s3.amazonaws.com/censortube-data/${video_id}/results.json`,
          final_callback
        );
      });
    });
  });
}


function processVideo(video_id, _callback) {
  updateVideoStatus(video_id, 'working', console.log);
  var url = 'https://www.youtube.com/watch?v=' + video_id;
  async.parallel({
    both: callback => getVideoUrl(video_id, callback)
  }, function(err, download_results) {
    if (err) return _callback(err, null);
    console.log('download done!');

    async.waterfall([
      function(callback) {
        getFlacFile(video_id, (err, res) => {console.log(err, res); callback(err,res);});
      },
      function(url, callback) {
        download(url).then(data => {
          var dest = './tmp/' + video_id + '-audio-final.flac';
          fs.writeFileSync(dest, data);
          callback(null, dest);
        })
      },
      function(best_audio_path, callback) {
        storageClient
          .bucket('censortube-audio-files')
          .upload(best_audio_path)
          .then(() => {
            //fs.removeSync(best_audio_path)
            callback(null, `gs://censortube-audio-files/${video_id}-audio-final.flac`, 44100)
          })
          .catch(err => {console.log(err);callback(err, null)});
      },
      function(gs_url, sample_rate, callback) {
        recognizeSpeech(gs_url, sample_rate, video_id, 1000, callback);
      }
    ], function (err, results) {
      updateVideoStatus(video_id, 'ready', console.log);
      _callback(err, {
        'video_link': download_results.both,
        'results_link': results
      })
    });
  });
}

function queryForVideo(video_id, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });

  var query_params = {
    TableName : "CensorTubeTable",
    KeyConditionExpression: "id = :vid",
    ExpressionAttributeValues: {
      ":vid": video_id
    }
  };

  docClient.query(query_params, function(err, data) {
    if (err) {
      console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
    } else {
      if (data.Items.length > 0) {
        var item = data.Items[0];
        callback(item.curr_state);
      } else {
        createVideoInfo(video_id, (err, data) => {
          processVideo(video_id, (err, results) => console.log('tis finished for now'));
          callback('not ready')
        })
      }
    }
  });
}

function createVideoInfo(video_id, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });
  var create_params = {
    TableName: "CensorTubeTable",
    Item: {
      id: video_id,
      curr_state: 'not ready',
      'results_link': 'null',
      'video_link': 'null'
    }
  };

  console.log("creating video item...")

  docClient.put(create_params, function(err, data) {
    if (err) {
      console.error("Unable to add item. Error JSON: ", JSON.stringify(err, null, 2));
    } else {
      console.log("Added item: ", JSON.stringify(data, null, 2));
    }
    callback(err, data);
  });
}

function updateVideoLink(video_id, link, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });

  var update_params = {
    TableName: "CensorTubeTable",
    Key: {
      id: video_id
    },
    UpdateExpression: "set video_link = :vlink",
    ExpressionAttributeValues:{
      ":vlink": link
    },
    ReturnValues:"UPDATED_NEW"
  };

  console.log("Updating the video item...");
  docClient.update(update_params, function(err, data) {
    if (err) {
      console.error("Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("UpdateItem succeeded:", JSON.stringify(data, null, 2));
    }
    callback(err, data);
  });
}

function updateResultsLink(video_id, link, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });

  var update_params = {
    TableName: "CensorTubeTable",
    Key: {
      id: video_id
    },
    UpdateExpression: "set results_link = :rlink",
    ExpressionAttributeValues:{
      ":rlink": link
    },
    ReturnValues:"UPDATED_NEW"
  };

  console.log("Updating the results item...");
  docClient.update(update_params, function(err, data) {
    if (err) {
      console.error("Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("UpdateItem succeeded:", JSON.stringify(data, null, 2));
    }
    callback(err, data);
  });
}

function updateVideoStatus(video_id, status, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });
  console.log(status);
  var update_params = {
    TableName: "CensorTubeTable",
    Key: {
      id: video_id
    },
    UpdateExpression: "set curr_state = :stat",
    ExpressionAttributeValues: {
      ":stat": status
    },
    ReturnValues:"UPDATED_NEW"
  };

  console.log("Updating the status item...");
  docClient.update(update_params, function(err, data) {
    if (err) {
      console.error("Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("UpdateItem succeeded:", JSON.stringify(data, null, 2));
    }
    callback(err, data);
  });
}

function getVideoInfo(video_id, callback) {
  var docClient = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });

  var params = {
    TableName: 'CensorTubeTable',
    Key:{
      id: video_id
    }
  };

  docClient.get(params, function(err, data) {
    if (err) {
      console.error("Unable to read item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("GetItem succeeded:", JSON.stringify(data, null, 2));
    }
    callback(err, data);
  });
}

function check(videoId, callback) {
  queryForVideo(videoId, state => {
    callback(null, {
      "statusCode": 200,
      "body": JSON.stringify({ state: state })
    });
  });
}

function retrieve(videoId, callback) {
  getVideoInfo(videoId, (err, data) => {
    callback(null, {
      "statusCode": 200,
      "body": JSON.stringify(data.Item)
    });
  })
}

exports.handler = function(event, context, callback) {
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