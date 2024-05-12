import React from 'react';
// import NProgress from 'nprogress';
// import Progressbars from './Progressbars';

export default {

  path: '/dashboard/progressbars',

  async action() {
    const Progressbars = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Progressbars').default), 'progressbars');
    });
    return <Progressbars key={'progressbars'} />;
  },

};
