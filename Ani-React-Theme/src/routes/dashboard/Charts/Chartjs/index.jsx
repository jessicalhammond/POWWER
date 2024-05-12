import React from 'react';
// import NProgress from 'nprogress';

export default {

  path: '/dashboard/chartjs',

  async action() {
    // NProgress.start();
    const Chartjs = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Chartjs').default), 'chartjs');
    });
    // NProgress.done();
    return <Chartjs key={'chartjs'} />;
  },

};
