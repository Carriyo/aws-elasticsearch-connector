const { Connection } = require('@elastic/elasticsearch')
const aws4 = require('aws4')
const AWS = require('aws-sdk')
const get = require('lodash.get')


module.exports = (awsConfig) => {
  class AmazonConnection extends Connection {
    constructor (options) {
      super(options)
      this.awsConfig = awsConfig || AWS.config
    }

    get credentials () {
      let credentials = {
        accessKeyId: get(this.awsConfig, 'credentials.accessKeyId'),
        secretAccessKey: get(this.awsConfig, 'credentials.secretAccessKey'),
        sessionToken: get(this.awsConfig, 'credentials.sessionToken')
      }
      
      // if config wasn't passed then fallback to access keys in process.env
      if (!credentials.accessKeyId && !credentials.secretAccessKey) {
        credentials = {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY,
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_KEY,
          sessionToken: process.env.AWS_SESSION_TOKEN
        }
      }
      
      if (!credentials.accessKeyId || !credentials.secretAccessKey) {
        throw new Error('Missing AWS credentials')
      }

      return credentials
    }

    buildRequestObject (params) {
      const req = super.buildRequestObject(params)

      if (!req.headers) {
        req.headers = {}
      }

      // Fix the Host header, since HttpConnector.makeReqParams() appends
      // the port number which will cause signature verification to fail
      req.headers.Host = req.hostname

      if (params.body) {
        req.headers['Content-Length'] = Buffer.byteLength(params.body, 'utf8')
        req.body = params.body
      } else {
        req.headers['Content-Length'] = 0
      }

      return aws4.sign(req, this.credentials)
    }
  }

  return AmazonConnection
}
