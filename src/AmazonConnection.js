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
      const credentials = {
        accessKeyId: get(this.awsConfig, 'credentials.accessKeyId', false),
        secretAccessKey: get(this.awsConfig, 'credentials.secretAccessKey', false),
        sessionToken: get(this.awsConfig, 'credentials.sessionToken')
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
