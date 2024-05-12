import React from 'react';
// import NProgress from 'nprogress';
// import Components from './Components';

export default {

  path: '/dashboard/components',

  async action() {
    const Components = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Components').default), 'components');
    });
    return <Components key={'formComponents'} />;
  },

};
