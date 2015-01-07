db.Words.mapReduce(
   function() {
       var letters = this.word.split('');
       letters = letters.sort();
       emit(letters, this.word);
   },  //map function
   function(key,values) {
        var all="";
        for(var i in values) {
            all+=values[i]+",";
        }
        return all;
   },
   {
      out: "words_analysis"
   }
);