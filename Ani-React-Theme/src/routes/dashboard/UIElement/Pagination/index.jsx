import React from 'react';
// import NProgress from 'nprogress';
// import Paginations from './Paginations';

export default {

  path: '/dashboard/paginations',

  async action() {
    const Paginations = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Paginations').default), 'paginations');
    });
    return <Paginations key={'paginations'} />;
  },

};
