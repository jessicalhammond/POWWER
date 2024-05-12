import React from 'react';
// import NProgress from 'nprogress';
// import DropDowns from './DropDowns';

export default {

  path: '/dashboard/dropdowns',

  async action() {
    const DropDowns = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./DropDowns').default), 'dropdowns');
    });
    return <DropDowns key={'dropdowns'} />;
  },

};
