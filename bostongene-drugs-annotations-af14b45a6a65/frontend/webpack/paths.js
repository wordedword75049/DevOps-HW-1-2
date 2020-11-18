const path = require('path');
const fs = require('fs');

const appDirectory = fs.realpathSync(process.cwd());
const resolveApp = relativePath => path.resolve(appDirectory, relativePath);

module.exports = {
  appPublic: resolveApp('public'), // Public files
  appBuild: resolveApp('build'), // Prod built files end up here
  appHtml: resolveApp('public/index.html'), // html file,
  appIndexTs: resolveApp('src/index.tsx'), // Main entry point
  appSrc: resolveApp('src') // App source
};
