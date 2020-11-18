/* eslint-disable import/no-extraneous-dependencies */
const { merge } = require('webpack-merge');
const ErrorOverlayPlugin = require('error-overlay-webpack-plugin');
const common = require('./webpack.common');

module.exports = merge(common, {
  plugins: [new ErrorOverlayPlugin()],
  mode: 'development',
  devtool: 'cheap-module-source-map',
  devServer: {
    historyApiFallback: true,
    hot: true,
    port: '3000',
    stats: 'minimal',
    open: true,
    proxy: {
      '/local': {
        target: 'http://localhost:5000',
        pathRewrite: {
          '^/local': ''
        }
      },
    }
  }
});
