import React from 'react';
// import NProgress from 'nprogress';
// import Alerts from './Alerts';

export default {

  path: '/dashboard/alerts',

  async action() {
    const Alerts = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Alerts').default), 'alerts');
    });
    return <Alerts key={'alerts'} />;
  },

};
