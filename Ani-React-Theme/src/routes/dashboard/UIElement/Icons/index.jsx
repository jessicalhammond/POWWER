import React from 'react';
// import NProgress from 'nprogress';
// import Icons from './Icons';

export default {

  path: '/dashboard/icons',

  async action() {
    const Icons = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Icons').default), 'icons');
    });
    return <Icons key={'icons'} />;
  },

};
