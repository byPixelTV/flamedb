const path = require('node:path');
const CopyPlugin = require('copy-webpack-plugin');
module.exports = {
  entry: './src/module.ts',
  output: { filename: 'module.js', path: path.resolve(__dirname, 'dist'), library: { type: 'amd' }, publicPath: 'auto' },
  resolve: { extensions: ['.ts', '.tsx', '.js'] },
  module: { rules: [{ test: /\.tsx?$/, loader: 'ts-loader', exclude: /node_modules/ }] },
  externals: [/^@grafana\//, 'react', 'react-dom', 'react/jsx-runtime'],
  plugins: [new CopyPlugin({ patterns: [
    { from: 'src/plugin.json', to: 'plugin.json' }, { from: 'src/img', to: 'img' },
    { from: 'dashboards', to: 'dashboards' }, { from: 'README.md', to: 'README.md' }
  ] })],
};
