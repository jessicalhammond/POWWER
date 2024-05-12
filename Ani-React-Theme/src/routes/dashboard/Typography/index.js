import React from 'react';
// import NProgress from 'nprogress';
// import Typography from './Typography';

export default {

  path: '/dashboard/typography',

  async action() {
    const Typography = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Typography').default), 'typography');
    });
    return <Typography key={'typography'} />;
  },

};
