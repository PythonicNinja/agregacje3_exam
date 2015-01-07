
// ENABLE ScopedThread
// link http://stackoverflow.com/questions/27247885/thread-and-scopedthread-functions-in-mongo-shell
// https://jira.mongodb.org/browse/SERVER-13485
// curl -O https://raw.githubusercontent.com/mongodb/mongo/master/jstests/libs/parallelTester.js
load("parallelTester.js");

var mapred = function(id, page) {
    return db.runCommand({
            mapreduce: "wikipedia",
            map: function () {
                if(this.hasOwnProperty('text')) {
                    var words = [];
                    for (var name_text in this.text) {
                        for (var i = 0; i < this.text[name_text].length; i++) {
                            words = words.concat(this.text[name_text][i].text.split(' '));
                        }
                    }
                    for (var i = 0; i <= words.length; i++)
                        emit(words[i], 1);
                }
            },
            reduce: function (key, values) { return Array.sum(values); },

            out: { replace: "mrout" + id, db: "mrdb" + id },
            sort: {_id: -1},
            query: { _id: { $lt: id} },
            limit: page
    })
};

var all = db.wikipedia.count();

var res = db.runCommand({splitVector: "pl_wikipedia.wikipedia", keyPattern: {_id: 1}, maxChunkSizeBytes: 4 * 1024 *1024 * 1024 });

threads = [];

var id_s = res.splitKeys;
var threads_num = id_s.length;
var page = all/threads_num;

for(var i=0; i<id_s.length;i++){
    print("id:" + id_s[i]._id);
    var t = new ScopedThread(mapred, id_s[i]._id, page);
    threads.push(t);
    t.start();
}

for (var i in threads) {
    var t = threads[i];
    t.join();
    printjson(t.returnData());
}