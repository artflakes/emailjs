var smtp     = require('./smtp');
var smtpError   = require('./error');
var message    = require('./message');
var address    = require('./address');
var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
/*
__UTC__ version for
  [formatting Date for mySQL](https://github.com/rentzsch/node-mysql-oil/blob/master/lib/node-mysql-oil.js#L237)
  heavily modified
*/
var dbTimestamp = function() {
  var date, day, month, time, zero, x;
  x = new Date();
  zero = function(n) {
    if (n > 9) {
      return n;
    } else {
      return "0" + n;
    }
  };
  month = x.getUTCMonth() + 1;
  day = x.getUTCDate();
  date = "" + (x.getUTCFullYear()) + "-" + (zero(month)) + "-" + (zero(day));
  time = "" + (zero(x.getUTCHours())) + ":" + (zero(x.getUTCMinutes())) + ":" + (zero(x.getUTCSeconds()));
  return "" + date + " " + time;
};


var Client = function(server, dbConfig)
{
  this.smtp      = new smtp.SMTP(server);

  //this.smtp.debug(1);

  this.queue      = [];
  this.timer      = null;
  this.sending    = false;

  if (typeof(dbConfig)==='object')
    this.oil =  require("mysql-oil").connect(dbConfig);
  else
    throw(new Error("missing db config"));
};

Client.prototype = 
{
  _poll: function()
  {
    var self = this;

    clearTimeout(self.timer);

    if(self.queue.length)
    {
      if(self.smtp.state() == smtp.state.NOTCONNECTED)
        self._connect(self.queue[0]);

      else if(self.smtp.state() == smtp.state.CONNECTED && !self.sending)
        self._sendmail(self.queue.shift());
    }
    // wait around 1 seconds in case something does come in, otherwise close out SMTP connection
    else
      self.timer = setTimeout(function() { self.smtp.quit(); }, 1000);
  },

  _connect: function(stack)
  {
    var self = this,

    connect = function(err)
    {
      if(!err)
      {
        var login = function(err)
        {
          if(!err)
            self._poll();

          else
            stack.callback(err, stack.message);
        };

        if(!self.smtp.authorized())
          self.smtp.login(login);

        else
          self._poll();
      }
      else
        stack.callback(err, stack.message);
    };

    self.smtp.connect(connect);
  },

  send: function(msg, callback)
  {
    var self = this;

    if(!(msg instanceof message.Message) && msg.from && msg.to && msg.text)
      msg = message.create(msg);

    if(msg instanceof message.Message && msg.valid())
    {
      var stack = 
      {
        message:    msg,
        to:      address.parse(msg.header["to"]),
        from:      address.parse(msg.header["from"])[0].address,
        callback:  callback || function() {},
        uuid: msg.header["uuid"] ? msg.header["uuid"] : ''
      };

      if(msg.header["cc"])
        stack.to = stack.to.concat(address.parse(msg.header["cc"]));

      if(msg.header["bcc"])
        stack.to = stack.to.concat(address.parse(msg.header["bcc"]));

      this.oil({ insert_into: 'mailqueue',
            values: { created_at: dbTimestamp(),
                      uuid: stack.uuid,
                      content: JSON.stringify(stack)
                    },
            cb: __bind(function(err,info)
            {
              if(!err)
              {
                stack["db_id"] = info.insertId;
                self.queue.push(stack);
                self._poll();
              }
              else
              {
                console.log( err )
                console.log( info )
                callback({code:-1, message:"couldn't store into the database"}, msg);
              }
            })
      });
      
    }
    else
      callback({code:-1, message:"message is not a valid Message instance"}, msg);
  },
  _sendsmtp: function(stack, next)
  {
    var self  = this;
    var check= function(err)
    {
      if(!err && next)
        next.apply(self, [stack]);

      else
        stack.callback(err, stack.message);
    };

    return check;
  },

  _sendmail: function(stack)
  {
    var self = this;
    self.sending = true;
    self.smtp.mail(self._sendsmtp(stack, self._sendrcpt), '<' + stack.from + '>');
  },

  _sendrcpt: function(stack)
  {
    var self = this, to = stack.to.shift().address;
    self.smtp.rcpt(self._sendsmtp(stack, stack.to.length ? self._sendrcpt : self._senddata), '<'+ to +'>');
  },

  _senddata: function(stack)
  {
    var self = this;
    self.smtp.data(self._sendsmtp(stack, self._sendmessage));
  },

  _sendmessage: function(stack)
  {
    var self = this, stream = stack.message.stream();

    stream.on('data', function(data) { self.smtp.message(data); });
    stream.on('end', function() { self.smtp.data_end(self._sendsmtp(stack, self._senddone)); });
    stream.on('error', self._sendsmtp(stack));
  },

  _senddone: function(stack)
  {
    this.oil({ update: 'mailqueue',
      values: { send_at: dbTimestamp() },
      where: ['id = ?', stack.db_id],
      cb: function(err,rows){}
    });
    var self = this;
        self.sending = false;
        stack.callback(null, stack.message);
        self._poll();
  }
};

exports.Client = Client;

exports.connect = function(server, dbconfig)
{
  return new Client(server, dbconfig);
}

