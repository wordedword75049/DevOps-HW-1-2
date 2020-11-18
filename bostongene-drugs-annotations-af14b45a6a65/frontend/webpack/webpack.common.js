/* eslint-disable import/no-extraneous-dependencies */
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const DotenvPlugin = require('dotenv-webpack');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const SimpleProgressWebpackPlugin = require('simple-progress-webpack-plugin');
const webpack = require('webpack');
const path = require('path');
const paths = require('./paths');

const styleLoader =
  process.env.NODE_ENV === 'production' ? MiniCssExtractPlugin.loader : 'style-loader';

module.exports = {
  entry: [paths.appIndexTs],
  output: {
    path: paths.appBuild,
    publicPath: '/',
    filename: 'static/js/[name].[hash].js'
  },
  module: {
    rules: [
      {
        test: /\.(png|svg|jpg)$/,
        use: ['file-loader']
      },
      {
        test: /\.scss$/,
        use: [
          styleLoader,
          {
            loader: 'css-loader',
            options: {
              importLoaders: 2
            }
          },
          'postcss-loader',
          {
            loader: 'sass-loader',
            options: {
              sassOptions: { includePaths: ['src'] }
            }
          }
        ]
      },
      {
        test: /\.css$/,
        use: [
          styleLoader,
          {
            loader: 'css-loader',
            options: {
              importLoaders: 1
            }
          },
          'postcss-loader'
        ]
      },
      {
        test: /\.(j|t)sx?$/,
        exclude: /(node_modules)/,
        loader: 'babel-loader'
      }
    ]
  },
  resolve: {
    extensions: ['.jsx', '.js', '.ts', '.tsx'],
    modules: [paths.appSrc, 'node_modules'],
    alias: {
      styles: path.resolve(paths.appSrc, 'styles'),
      pages: path.resolve(paths.appSrc, 'pages')
    }
  },
  plugins: [
    new CleanWebpackPlugin(),
    // new CopyWebpackPlugin([{ from: 'public/assets', to: 'assets' }]),
    new DotenvPlugin(),
    new HtmlWebpackPlugin({
      template: paths.appHtml
    }),
    new MiniCssExtractPlugin({
      filename: 'static/css/[name].[hash].css'
    }),
    new SimpleProgressWebpackPlugin({
      format: 'compact'
    }),
    new webpack.EnvironmentPlugin({}),
    new webpack.LoaderOptionsPlugin({
      debug: false
    })
  ]
};
