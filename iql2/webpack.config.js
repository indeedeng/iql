var webpack = require('webpack');

var config = {
  entry: "./dist/src/Parsers",
  output: {
    path: 'dist',
    filename: 'bundle.js',
    libraryTarget: 'umd'
  },
  module: {
    loaders: [
      {
        test: /\.jsx?$/,
        exclude: /(node_modules|bower_components|gen-src)/,
        loader: 'babel',
        query: {stage: 0}
      }
    ]
  },
  resolve: {
    extensions: ["",".js",".jsx"],
  },
  devtool: 'source-map',
  externals: [
    'moment',
    /antlr4(\/.*)?/
  ]
};

if (process.env.MINIFY) {
  config.plugins = [
    new webpack.optimize.DedupePlugin(),
    new webpack.optimize.OccurenceOrderPlugin(),
  ];
}

module.exports = config;
