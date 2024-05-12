import React from 'react';
// import NProgress from 'nprogress';
// import Buttons from './Buttons';

export default {

  path: '/dashboard/buttons',

  async action() {
    const Buttons = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Buttons').default), 'buttons');
    });
    return <Buttons key={'buttons'} />;
  },

};
