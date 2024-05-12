import React from 'react';
// import NProgress from 'nprogress';
// import Inbox from './Inbox';

export default {

  path: '/dashboard/inbox',

  async action() {
    const Inbox = await new Promise((resolve) => {
      require.ensure([], (require) => resolve(require('./Inbox').default), 'inbox');
    });
    return <Inbox key={'inbox'} />;
  },

};
