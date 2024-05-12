import React from 'react';
// import NProgress from 'nprogress';
// import Grid from './Grid';

export default {

  path: '/dashboard/grid',

  async action() {
    // NProgress.start();
    // if (process.browser) {
    //   console.log('inside nprogress start');
    //   NProgress.start();
    // }
    const Grid = await new Promise((resolve) => {
      require.ensure([], (require) => {
        // if (process.browser) {
        //   console.log('inside nprogress done');
        //   require('nprogress').done();
        // }
        resolve(require('./Grid').default);
      }, 'grid');
    });
    // NProgress.done();
    return <Grid key={'gird'} />;
  },

};
