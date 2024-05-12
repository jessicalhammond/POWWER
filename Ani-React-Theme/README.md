## Ani Theme React.js Version

Thanks for purchasing the Ani Theme React.js Version.
- Live Demo URL: http://ani-react-theme.geekydev.com:3000/
- Product URL: http://www.strapui.com/themes/ani-reactjs-theme/

Follow the documentation to install and get started with the development:

## Get Started

###1. System Requirements

* Globally installed [node](https://nodejs.org/en/) >= 5.0 < 7.0

* Globally installed [npm](https://www.npmjs.org/) >= 3.0

###2. Installation

```shell
$ git clone gitlab@gitlab.sahusoft.com:products/ani-react-theme.git -b master
$ cd ani-react-theme.git
$ npm install
```
This will install both run-time project dependencies and developer tools listed
in [package.json] file.

###3. Run

```shell
$ npm start
```
This command will build the app from the source files (`/src`) into the output
`/build` folder. As soon as the initial build completes, it will start the
Node.js server (`node build/server.js`) and [Browsersync](https://browsersync.io/)
with [HMR](https://webpack.github.io/docs/hot-module-replacement) on top of it.

> [http://localhost:3000/](http://localhost:3000/) — Node.js server (`build/server.js`)<br>
> [http://localhost:3001/](http://localhost:3001/) — BrowserSync proxy with HMR, React Hot Transform<br>

Now you can open your web app in a browser, on mobile devices and start
hacking. Whenever you modify any of the source files inside the `/src` folder,
the module bundler ([Webpack](http://webpack.github.io/)) will recompile the
app on the fly and refresh all the connected browsers.

Note that the `npm start` command launches the app in `development` mode,
the compiled output files are not optimized and minimized in this case.
You can use `--release` command line argument to check how your app works
in release (production) mode:

```shell
$ npm start -- --release
```
###4. How to Build, Deploy

If you need just to build the app (without running a dev server), simply run:

```shell
$ npm run build
```

or, for a production build:

```shell
$ npm run build -- --release
```

After running this command, the `/build` folder will contain the compiled
version of the app. For example, you can launch Node.js server normally by
running `node build/server.js`.

Happy coding!
